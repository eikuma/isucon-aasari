package main

import (
	"bytes"
	"compress/gzip"
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	gsm "github.com/bradleypeabody/gorilla-sessions-memcache"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/sessions"
	"github.com/jmoiron/sqlx"
)

var (
	db           *sqlx.DB
	store        *gsm.MemcacheStore
	memcacheConn *memcache.Client
)

const (
	postsPerPage  = 20
	ISO8601Format = "2006-01-02T15:04:05-07:00"
	UploadLimit   = 10 * 1024 * 1024 // 10mb
)

type User struct {
	ID          int       `db:"id"`
	AccountName string    `db:"account_name"`
	Passhash    string    `db:"passhash"`
	Authority   int       `db:"authority"`
	DelFlg      int       `db:"del_flg"`
	CreatedAt   time.Time `db:"created_at"`
}

type Post struct {
	ID           int       `db:"id"`
	UserID       int       `db:"user_id"`
	Imgdata      []byte    `db:"imgdata"`
	Body         string    `db:"body"`
	Mime         string    `db:"mime"`
	CreatedAt    time.Time `db:"created_at"`
	CommentCount int
	Comments     []Comment
	User         User
	CSRFToken    string
}

type Comment struct {
	ID        int       `db:"id"`
	PostID    int       `db:"post_id"`
	UserID    int       `db:"user_id"`
	Comment   string    `db:"comment"`
	CreatedAt time.Time `db:"created_at"`
	User      User
}

func init() {
	memdAddr := os.Getenv("ISUCONP_MEMCACHED_ADDRESS")
	if memdAddr == "" {
		memdAddr = "localhost:11211"
	}
	memcacheConn = memcache.New(memdAddr)

	// Configure memcache connection settings for better performance
	memcacheConn.Timeout = 100 * time.Millisecond
	memcacheConn.MaxIdleConns = 50

	// Test memcache connection
	if err := memcacheConn.Ping(); err != nil {
		log.Printf("Warning: Failed to connect to memcache at %s: %v", memdAddr, err)
	} else {
		log.Printf("Successfully connected to memcache at %s", memdAddr)
	}

	store = gsm.NewMemcacheStore(memcacheConn, "iscogram_", []byte("sendagaya"))
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

func dbInitialize() {
	sqls := []string{
		"DELETE FROM users WHERE id > 1000",
		"DELETE FROM posts WHERE id > 10000",
		"DELETE FROM comments WHERE id > 100000",
		"UPDATE users SET del_flg = 0",
		"UPDATE users SET del_flg = 1 WHERE id % 50 = 0",
	}

	for _, sql := range sqls {
		db.Exec(sql)
	}

	// Clear memcache on initialization
	memcacheConn.FlushAll()
}

// Cache helper functions with improved TTL strategy
func getCacheKey(prefix string, params ...interface{}) string {
	key := prefix
	for _, param := range params {
		key += fmt.Sprintf("_%v", param)
	}
	return key
}

func getUserFromCache(userID int) (*User, error) {
	key := getCacheKey("user", userID)
	item, err := memcacheConn.Get(key)
	if err != nil {
		return nil, err
	}

	var user User
	if err := json.Unmarshal(item.Value, &user); err != nil {
		return nil, err
	}
	return &user, nil
}

func setUserCache(user *User) error {
	key := getCacheKey("user", user.ID)
	data, err := json.Marshal(user)
	if err != nil {
		return err
	}

	return memcacheConn.Set(&memcache.Item{
		Key:        key,
		Value:      data,
		Expiration: 3600, // Extended to 1 hour for user data (users rarely change)
	})
}

func getCommentCountFromCache(postID int) (int, error) {
	key := getCacheKey("comment_count", postID)
	item, err := memcacheConn.Get(key)
	if err != nil {
		return 0, err
	}

	var count int
	if err := json.Unmarshal(item.Value, &count); err != nil {
		return 0, err
	}
	return count, nil
}

func setCommentCountCache(postID, count int) error {
	key := getCacheKey("comment_count", postID)
	data, err := json.Marshal(count)
	if err != nil {
		return err
	}

	return memcacheConn.Set(&memcache.Item{
		Key:        key,
		Value:      data,
		Expiration: 600, // Extended to 10 minutes for comment counts
	})
}

func setImageCache(postID int, data []byte, mime string) error {
	key := getCacheKey("image", postID)
	imageData := struct {
		Data []byte `json:"data"`
		Mime string `json:"mime"`
	}{
		Data: data,
		Mime: mime,
	}

	jsonData, err := json.Marshal(imageData)
	if err != nil {
		return err
	}

	// Always compress image data for better cache efficiency
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(jsonData); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}

	finalData := buf.Bytes()
	// Always use compression flag for consistency
	key += "_gz"

	return memcacheConn.Set(&memcache.Item{
		Key:        key,
		Value:      finalData,
		Expiration: 7200, // Extended to 2 hours for images (they rarely change)
	})
}

func getImageFromCache(postID int) ([]byte, string, error) {
	// Always try compressed version (consistency with setImageCache)
	key := getCacheKey("image", postID) + "_gz"
	item, err := memcacheConn.Get(key)
	if err != nil {
		return nil, "", err
	}

	// Decompress
	gz, err := gzip.NewReader(bytes.NewReader(item.Value))
	if err != nil {
		return nil, "", err
	}
	defer gz.Close()

	decompressed, err := io.ReadAll(gz)
	if err != nil {
		return nil, "", err
	}

	var imageData struct {
		Data []byte `json:"data"`
		Mime string `json:"mime"`
	}
	if err := json.Unmarshal(decompressed, &imageData); err != nil {
		return nil, "", err
	}
	return imageData.Data, imageData.Mime, nil
}

// New: Cache for post lists
func getPostsFromCache(cacheKey string) ([]Post, error) {
	item, err := memcacheConn.Get(cacheKey)
	if err != nil {
		return nil, err
	}

	var posts []Post
	if err := json.Unmarshal(item.Value, &posts); err != nil {
		return nil, err
	}
	return posts, nil
}

func setPostsCache(cacheKey string, posts []Post) error {
	data, err := json.Marshal(posts)
	if err != nil {
		return err
	}

	return memcacheConn.Set(&memcache.Item{
		Key:        cacheKey,
		Value:      data,
		Expiration: 300, // Reduced to 5 minutes for more frequent updates
	})
}

// New: Cache for user statistics
func getUserStatsFromCache(userID int) (map[string]int, error) {
	key := getCacheKey("user_stats", userID)
	item, err := memcacheConn.Get(key)
	if err != nil {
		return nil, err
	}

	var stats map[string]int
	if err := json.Unmarshal(item.Value, &stats); err != nil {
		return nil, err
	}
	return stats, nil
}

func setUserStatsCache(userID int, stats map[string]int) error {
	key := getCacheKey("user_stats", userID)
	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}

	return memcacheConn.Set(&memcache.Item{
		Key:        key,
		Value:      data,
		Expiration: 600, // 10 minutes for user statistics
	})
}

func tryLogin(accountName, password string) *User {
	u := User{}
	err := db.Get(&u, "SELECT * FROM users WHERE account_name = ? AND del_flg = 0", accountName)
	if err != nil {
		return nil
	}

	if calculatePasshash(u.AccountName, password) == u.Passhash {
		return &u
	} else {
		return nil
	}
}

func validateUser(accountName, password string) bool {
	return regexp.MustCompile(`\A[0-9a-zA-Z_]{3,}\z`).MatchString(accountName) &&
		regexp.MustCompile(`\A[0-9a-zA-Z_]{6,}\z`).MatchString(password)
}

// 今回のGo実装では言語側のエスケープの仕組みが使えないのでOSコマンドインジェクション対策できない
// 取り急ぎPHPのescapeshellarg関数を参考に自前で実装
// cf: http://jp2.php.net/manual/ja/function.escapeshellarg.php
func escapeshellarg(arg string) string {
	return "'" + strings.Replace(arg, "'", "'\\''", -1) + "'"
}

func digest(src string) string {
	// opensslのバージョンによっては (stdin)= というのがつくので取る
	out, err := exec.Command("/bin/bash", "-c", `printf "%s" `+escapeshellarg(src)+` | openssl dgst -sha512 | sed 's/^.*= //'`).Output()
	if err != nil {
		log.Print(err)
		return ""
	}

	return strings.TrimSuffix(string(out), "\n")
}

func calculateSalt(accountName string) string {
	return digest(accountName)
}

func calculatePasshash(accountName, password string) string {
	return digest(password + ":" + calculateSalt(accountName))
}

func getSession(r *http.Request) *sessions.Session {
	session, _ := store.Get(r, "isuconp-go.session")

	return session
}

func getSessionUser(r *http.Request) User {
	session := getSession(r)
	uid, ok := session.Values["user_id"]
	if !ok || uid == nil {
		return User{}
	}

	// Try to get user from session cache first
	sessionID := session.ID
	if sessionID != "" {
		if user, err := getUserSessionFromCache(sessionID); err == nil {
			return *user
		}
	}

	// Try to get user from cache first
	userID, ok := uid.(int64)
	if !ok {
		return User{}
	}

	if user, err := getUserFromCache(int(userID)); err == nil {
		// Cache in session for faster future access
		if sessionID != "" {
			setUserSessionCache(sessionID, user)
		}
		return *user
	}

	// Cache miss, fetch from database
	var u User
	err := db.Get(&u, "SELECT * FROM `users` WHERE `id` = ?", uid)
	if err != nil {
		return User{}
	}
	// Store in cache for next time
	if err := setUserCache(&u); err != nil {
		log.Printf("Failed to cache user %d: %v", u.ID, err)
	}
	// Also cache in session
	if sessionID != "" {
		setUserSessionCache(sessionID, &u)
	}
	return u
}

func getFlash(w http.ResponseWriter, r *http.Request, key string) string {
	session := getSession(r)
	value, ok := session.Values[key]

	if !ok || value == nil {
		return ""
	} else {
		delete(session.Values, key)
		session.Save(r, w)
		return value.(string)
	}
}

func makePosts(results []Post, csrfToken string, allComments bool) ([]Post, error) {
	if len(results) == 0 {
		return []Post{}, nil
	}

	// Extract post IDs for batch operations
	postIDs := make([]int, len(results))
	for i, p := range results {
		postIDs[i] = p.ID
	}

	// Create IN clause placeholder
	placeholder := strings.Repeat("?,", len(postIDs)-1) + "?"
	args := make([]interface{}, len(postIDs))
	for i, id := range postIDs {
		args[i] = id
	}

	// 1. Get comment counts using LEFT JOIN (with cache fallback)
	type CommentCountResult struct {
		PostID int `db:"post_id"`
		Count  int `db:"count"`
	}
	commentCounts := make(map[int]int)

	// Try to get counts from cache first
	missedPostIDs := []int{}
	for _, postID := range postIDs {
		if count, err := getCommentCountFromCache(postID); err == nil {
			commentCounts[postID] = count
		} else {
			missedPostIDs = append(missedPostIDs, postID)
		}
	}

	// Fetch missed counts from database in batch
	if len(missedPostIDs) > 0 {
		missedPlaceholder := strings.Repeat("?,", len(missedPostIDs)-1) + "?"
		missedArgs := make([]interface{}, len(missedPostIDs))
		for i, id := range missedPostIDs {
			missedArgs[i] = id
		}

		var countResults []CommentCountResult
		countQuery := fmt.Sprintf(`
			SELECT p.id AS post_id, COUNT(c.id) AS count
			FROM posts p
			LEFT JOIN comments c ON p.id = c.post_id
			WHERE p.id IN (%s)
			GROUP BY p.id`, missedPlaceholder)

		err := db.Select(&countResults, countQuery, missedArgs...)
		if err != nil {
			return nil, err
		}

		for _, cr := range countResults {
			commentCounts[cr.PostID] = cr.Count
			// Cache the result
			if err := setCommentCountCache(cr.PostID, cr.Count); err != nil {
				log.Printf("Failed to cache comment count for post %d: %v", cr.PostID, err)
			}
		}
	}

	// 2. Get comments with users using optimized query
	type CommentWithUser struct {
		Comment
		CommentUserID int       `db:"comment_user_id"`
		UserAccount   string    `db:"user_account_name"`
		UserPassword  string    `db:"user_passhash"`
		UserAuthority int       `db:"user_authority"`
		UserDelFlg    int       `db:"user_del_flg"`
		UserCreated   time.Time `db:"user_created_at"`
	}

	commentsMap := make(map[int][]Comment)
	var commentResults []CommentWithUser

	// Use a more efficient query that leverages indexes better
	var commentsQuery string
	if allComments {
		commentsQuery = fmt.Sprintf(`
			SELECT 
				c.id, c.post_id, c.user_id, c.comment, c.created_at,
				u.id AS comment_user_id, u.account_name AS user_account_name, 
				u.passhash AS user_passhash, u.authority AS user_authority,
				u.del_flg AS user_del_flg, u.created_at AS user_created_at
			FROM comments c
			INNER JOIN users u ON c.user_id = u.id
			WHERE c.post_id IN (%s)
			ORDER BY c.post_id ASC, c.created_at DESC`, placeholder)
	} else {
		// More efficient approach using LIMIT per post
		commentsQuery = fmt.Sprintf(`
			SELECT 
				c.id, c.post_id, c.user_id, c.comment, c.created_at,
				u.id AS comment_user_id, u.account_name AS user_account_name, 
				u.passhash AS user_passhash, u.authority AS user_authority,
				u.del_flg AS user_del_flg, u.created_at AS user_created_at
			FROM (
				SELECT c1.*
				FROM comments c1
				WHERE c1.post_id IN (%s)
				AND (
					SELECT COUNT(*)
					FROM comments c2
					WHERE c2.post_id = c1.post_id AND c2.created_at >= c1.created_at
				) <= 3
				ORDER BY c1.post_id ASC, c1.created_at DESC
			) c
			INNER JOIN users u ON c.user_id = u.id
			ORDER BY c.post_id ASC, c.created_at DESC`, placeholder)
	}

	err := db.Select(&commentResults, commentsQuery, args...)
	if err != nil {
		return nil, err
	}

	for _, cr := range commentResults {
		comment := Comment{
			ID:        cr.Comment.ID,
			PostID:    cr.Comment.PostID,
			UserID:    cr.Comment.UserID,
			Comment:   cr.Comment.Comment,
			CreatedAt: cr.Comment.CreatedAt,
			User: User{
				ID:          cr.CommentUserID,
				AccountName: cr.UserAccount,
				Passhash:    cr.UserPassword,
				Authority:   cr.UserAuthority,
				DelFlg:      cr.UserDelFlg,
				CreatedAt:   cr.UserCreated,
			},
		}
		commentsMap[cr.PostID] = append(commentsMap[cr.PostID], comment)

		// Cache the user data
		if err := setUserCache(&comment.User); err != nil {
			log.Printf("Failed to cache comment user %d: %v", comment.User.ID, err)
		}
	}

	// 3. Get post users using optimized batch cache operations
	postUsersMap := make(map[int]User)

	// Extract unique user IDs
	uniqueUserIDs := make(map[int]bool)
	for _, p := range results {
		uniqueUserIDs[p.UserID] = true
	}

	userIDsList := make([]int, 0, len(uniqueUserIDs))
	for id := range uniqueUserIDs {
		userIDsList = append(userIDsList, id)
	}

	// Try batch cache get first
	cachedUsers, missedUserIDs := batchGetUsersFromCache(userIDsList)

	// Map cached users to posts
	for _, p := range results {
		if user, found := cachedUsers[p.UserID]; found {
			postUsersMap[p.ID] = *user
		}
	}

	// Fetch missed users from database in batch
	if len(missedUserIDs) > 0 {
		// Remove duplicates (already done in batchGetUsersFromCache)
		userArgs := make([]interface{}, len(missedUserIDs))
		for i, id := range missedUserIDs {
			userArgs[i] = id
		}

		userPlaceholder := strings.Repeat("?,", len(missedUserIDs)-1) + "?"

		type PostUserResult struct {
			PostID        int       `db:"post_id"`
			UserID        int       `db:"user_id"`
			UserAccount   string    `db:"user_account_name"`
			UserPassword  string    `db:"user_passhash"`
			UserAuthority int       `db:"user_authority"`
			UserDelFlg    int       `db:"user_del_flg"`
			UserCreated   time.Time `db:"user_created_at"`
		}

		var userResults []PostUserResult
		userQuery := fmt.Sprintf(`
			SELECT 
				p.id AS post_id, u.id AS user_id, u.account_name AS user_account_name,
				u.passhash AS user_passhash, u.authority AS user_authority,
				u.del_flg AS user_del_flg, u.created_at AS user_created_at
			FROM posts p
			INNER JOIN users u ON p.user_id = u.id
			WHERE u.id IN (%s)`, userPlaceholder)

		err = db.Select(&userResults, userQuery, userArgs...)
		if err != nil {
			return nil, err
		}

		users := make([]User, 0, len(userResults))
		for _, ur := range userResults {
			user := User{
				ID:          ur.UserID,
				AccountName: ur.UserAccount,
				Passhash:    ur.UserPassword,
				Authority:   ur.UserAuthority,
				DelFlg:      ur.UserDelFlg,
				CreatedAt:   ur.UserCreated,
			}

			// Map to posts that use this user
			for _, p := range results {
				if p.UserID == user.ID {
					postUsersMap[p.ID] = user
				}
			}

			users = append(users, user)
		}

		// Batch cache the users
		batchSetUsersCache(users)
	}

	// 4. Assemble the final posts
	var posts []Post
	for _, p := range results {
		// Set comment count
		p.CommentCount = commentCounts[p.ID]

		// Set comments (reverse order for display)
		comments := commentsMap[p.ID]
		for i, j := 0, len(comments)-1; i < j; i, j = i+1, j-1 {
			comments[i], comments[j] = comments[j], comments[i]
		}
		p.Comments = comments

		// Set user
		if user, exists := postUsersMap[p.ID]; exists {
			p.User = user
		}

		p.CSRFToken = csrfToken

		// Add post to results (deleted user check already done in query)
		posts = append(posts, p)
		if len(posts) >= postsPerPage {
			break
		}
	}

	return posts, nil
}

func imageURL(p Post) string {
	ext := ""
	if p.Mime == "image/jpeg" {
		ext = ".jpg"
	} else if p.Mime == "image/png" {
		ext = ".png"
	} else if p.Mime == "image/gif" {
		ext = ".gif"
	}

	return "/image/" + strconv.Itoa(p.ID) + ext
}

func isLogin(u User) bool {
	return u.ID != 0
}

func getCSRFToken(r *http.Request) string {
	session := getSession(r)
	csrfToken, ok := session.Values["csrf_token"]
	if !ok {
		return ""
	}
	return csrfToken.(string)
}

func secureRandomStr(b int) string {
	k := make([]byte, b)
	if _, err := crand.Read(k); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", k)
}

func getTemplPath(filename string) string {
	return path.Join("templates", filename)
}

func getInitialize(w http.ResponseWriter, r *http.Request) {
	dbInitialize()
	w.WriteHeader(http.StatusOK)
}

func getLogin(w http.ResponseWriter, r *http.Request) {
	me := getSessionUser(r)

	if isLogin(me) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	template.Must(template.ParseFiles(
		getTemplPath("layout.html"),
		getTemplPath("login.html")),
	).Execute(w, struct {
		Me    User
		Flash string
	}{me, getFlash(w, r, "notice")})
}

func postLogin(w http.ResponseWriter, r *http.Request) {
	if isLogin(getSessionUser(r)) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	u := tryLogin(r.FormValue("account_name"), r.FormValue("password"))

	if u != nil {
		session := getSession(r)
		session.Values["user_id"] = u.ID
		session.Values["csrf_token"] = secureRandomStr(16)
		session.Save(r, w)

		http.Redirect(w, r, "/", http.StatusFound)
	} else {
		session := getSession(r)
		session.Values["notice"] = "アカウント名かパスワードが間違っています"
		session.Save(r, w)

		http.Redirect(w, r, "/login", http.StatusFound)
	}
}

func getRegister(w http.ResponseWriter, r *http.Request) {
	if isLogin(getSessionUser(r)) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	template.Must(template.ParseFiles(
		getTemplPath("layout.html"),
		getTemplPath("register.html")),
	).Execute(w, struct {
		Me    User
		Flash string
	}{User{}, getFlash(w, r, "notice")})
}

func postRegister(w http.ResponseWriter, r *http.Request) {
	if isLogin(getSessionUser(r)) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	accountName, password := r.FormValue("account_name"), r.FormValue("password")

	validated := validateUser(accountName, password)
	if !validated {
		session := getSession(r)
		session.Values["notice"] = "アカウント名は3文字以上、パスワードは6文字以上である必要があります"
		session.Save(r, w)

		http.Redirect(w, r, "/register", http.StatusFound)
		return
	}

	exists := 0
	// ユーザーが存在しない場合はエラーになるのでエラーチェックはしない
	db.Get(&exists, "SELECT 1 FROM users WHERE `account_name` = ?", accountName)

	if exists == 1 {
		session := getSession(r)
		session.Values["notice"] = "アカウント名がすでに使われています"
		session.Save(r, w)

		http.Redirect(w, r, "/register", http.StatusFound)
		return
	}

	query := "INSERT INTO `users` (`account_name`, `passhash`) VALUES (?,?)"
	result, err := db.Exec(query, accountName, calculatePasshash(accountName, password))
	if err != nil {
		log.Print(err)
		return
	}

	session := getSession(r)
	uid, err := result.LastInsertId()
	if err != nil {
		log.Print(err)
		return
	}
	session.Values["user_id"] = uid
	session.Values["csrf_token"] = secureRandomStr(16)
	session.Save(r, w)

	http.Redirect(w, r, "/", http.StatusFound)
}

func getLogout(w http.ResponseWriter, r *http.Request) {
	session := getSession(r)
	delete(session.Values, "user_id")
	session.Options = &sessions.Options{MaxAge: -1}
	session.Save(r, w)

	http.Redirect(w, r, "/", http.StatusFound)
}

func getIndex(w http.ResponseWriter, r *http.Request) {
	session := getSession(r)
	me := getSessionUser(r)

	// Try to get posts from cache first
	cacheKey := "index_posts"
	posts, err := getPostsFromCache(cacheKey)
	if err != nil {
		// Cache miss, fetch from database
		results := []Post{}

		// Only fetch posts from non-deleted users and limit to what we actually need
		// This pre-filters deleted users and limits the dataset early
		err := db.Select(&results, `
			SELECT p.id, p.user_id, p.body, p.mime, p.created_at 
			FROM posts p 
			INNER JOIN users u ON p.user_id = u.id 
			WHERE u.del_flg = 0 
			ORDER BY p.created_at DESC 
			LIMIT ?`, postsPerPage)
		if err != nil {
			log.Print(err)
			return
		}

		// Get CSRF token from the already retrieved session
		var csrfToken string
		if token, ok := session.Values["csrf_token"]; ok {
			csrfToken = token.(string)
		}

		posts, err = makePosts(results, csrfToken, false)
		if err != nil {
			log.Print(err)
			return
		}

		// Cache the results
		setPostsCache(cacheKey, posts)
	}

	// Get CSRF token from the already retrieved session
	var csrfToken string
	if token, ok := session.Values["csrf_token"]; ok {
		csrfToken = token.(string)
	}

	// Update CSRF tokens for cached posts
	for i := range posts {
		posts[i].CSRFToken = csrfToken
	}

	// Get flash message from the already retrieved session
	var flash string
	if value, ok := session.Values["notice"]; ok && value != nil {
		flash = value.(string)
		delete(session.Values, "notice")
		session.Save(r, w)
	}

	fmap := template.FuncMap{
		"imageURL": imageURL,
	}

	template.Must(template.New("layout.html").Funcs(fmap).ParseFiles(
		getTemplPath("layout.html"),
		getTemplPath("index.html"),
		getTemplPath("posts.html"),
		getTemplPath("post.html"),
	)).Execute(w, struct {
		Posts     []Post
		Me        User
		CSRFToken string
		Flash     string
	}{posts, me, csrfToken, flash})
}

func getAccountName(w http.ResponseWriter, r *http.Request) {
	accountName := r.PathValue("accountName")
	user := User{}

	err := db.Get(&user, "SELECT * FROM `users` WHERE `account_name` = ? AND `del_flg` = 0", accountName)
	if err != nil {
		log.Print(err)
		return
	}

	if user.ID == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	results := []Post{}

	err = db.Select(&results, "SELECT `id`, `user_id`, `body`, `mime`, `created_at` FROM `posts` WHERE `user_id` = ? ORDER BY `created_at` DESC LIMIT ?", user.ID, postsPerPage)
	if err != nil {
		log.Print(err)
		return
	}

	posts, err := makePosts(results, getCSRFToken(r), false)
	if err != nil {
		log.Print(err)
		return
	}

	// Try to get user statistics from cache first
	var commentCount, postCount, commentedCount int
	if stats, err := getUserStatsFromCache(user.ID); err == nil {
		commentCount = stats["comment_count"]
		postCount = stats["post_count"]
		commentedCount = stats["commented_count"]
	} else {
		// Cache miss, calculate from database
		type UserStats struct {
			CommentCount   int `db:"comment_count"`
			PostCount      int `db:"post_count"`
			CommentedCount int `db:"commented_count"`
		}

		var stats UserStats
		err = db.Get(&stats, `
			SELECT 
				COALESCE(comment_stats.comment_count, 0) AS comment_count,
				COALESCE(post_stats.post_count, 0) AS post_count,
				COALESCE(commented_stats.commented_count, 0) AS commented_count
			FROM (SELECT 1) dummy
			LEFT JOIN (
				SELECT COUNT(*) AS comment_count 
				FROM comments 
				WHERE user_id = ?
			) comment_stats ON 1=1
			LEFT JOIN (
				SELECT COUNT(*) AS post_count 
				FROM posts 
				WHERE user_id = ?
			) post_stats ON 1=1
			LEFT JOIN (
				SELECT COUNT(*) AS commented_count 
				FROM comments c 
				INNER JOIN posts p ON c.post_id = p.id 
				WHERE p.user_id = ?
			) commented_stats ON 1=1`, user.ID, user.ID, user.ID)
		if err != nil {
			log.Print(err)
			return
		}

		commentCount = stats.CommentCount
		postCount = stats.PostCount
		commentedCount = stats.CommentedCount

		// Cache the results
		statsMap := map[string]int{
			"comment_count":   commentCount,
			"post_count":      postCount,
			"commented_count": commentedCount,
		}
		setUserStatsCache(user.ID, statsMap)
	}

	me := getSessionUser(r)

	fmap := template.FuncMap{
		"imageURL": imageURL,
	}

	template.Must(template.New("layout.html").Funcs(fmap).ParseFiles(
		getTemplPath("layout.html"),
		getTemplPath("user.html"),
		getTemplPath("posts.html"),
		getTemplPath("post.html"),
	)).Execute(w, struct {
		Posts          []Post
		User           User
		PostCount      int
		CommentCount   int
		CommentedCount int
		Me             User
	}{posts, user, postCount, commentCount, commentedCount, me})
}

func getPosts(w http.ResponseWriter, r *http.Request) {
	m, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Print(err)
		return
	}
	maxCreatedAt := m.Get("max_created_at")
	if maxCreatedAt == "" {
		return
	}

	t, err := time.Parse(ISO8601Format, maxCreatedAt)
	if err != nil {
		log.Print(err)
		return
	}

	results := []Post{}
	err = db.Select(&results, `
		SELECT p.id, p.user_id, p.body, p.mime, p.created_at 
		FROM posts p 
		INNER JOIN users u ON p.user_id = u.id 
		WHERE p.created_at <= ? AND u.del_flg = 0 
		ORDER BY p.created_at DESC 
		LIMIT ?`, t.Format(ISO8601Format), postsPerPage)
	if err != nil {
		log.Print(err)
		return
	}

	posts, err := makePosts(results, getCSRFToken(r), false)
	if err != nil {
		log.Print(err)
		return
	}

	if len(posts) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	fmap := template.FuncMap{
		"imageURL": imageURL,
	}

	template.Must(template.New("posts.html").Funcs(fmap).ParseFiles(
		getTemplPath("posts.html"),
		getTemplPath("post.html"),
	)).Execute(w, posts)
}

func getPostsID(w http.ResponseWriter, r *http.Request) {
	pidStr := r.PathValue("id")
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	me := getSessionUser(r)
	userID := 0
	if me.ID != 0 {
		userID = me.ID
	}

	// Try to get cached page first
	if cachedHTML, err := getPostPageFromCache(pid, userID); err == nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(cachedHTML))
		return
	}

	results := []Post{}
	// Only fetch necessary fields, not the large imgdata field
	err = db.Select(&results, "SELECT id, user_id, body, mime, created_at FROM `posts` WHERE `id` = ?", pid)
	if err != nil {
		log.Print(err)
		return
	}

	posts, err := makePosts(results, getCSRFToken(r), true)
	if err != nil {
		log.Print(err)
		return
	}

	if len(posts) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	p := posts[0]

	fmap := template.FuncMap{
		"imageURL": imageURL,
	}

	// Render to buffer first for caching
	var buf bytes.Buffer
	err = template.Must(template.New("layout.html").Funcs(fmap).ParseFiles(
		getTemplPath("layout.html"),
		getTemplPath("post_id.html"),
		getTemplPath("post.html"),
	)).Execute(&buf, struct {
		Post Post
		Me   User
	}{p, me})

	if err != nil {
		log.Print(err)
		return
	}

	// Cache the rendered page
	htmlContent := buf.String()
	if err := setPostPageCache(pid, userID, htmlContent); err != nil {
		log.Printf("Failed to cache post page %d for user %d: %v", pid, userID, err)
	}

	// Send response
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(buf.Bytes())
}

func postIndex(w http.ResponseWriter, r *http.Request) {
	me := getSessionUser(r)
	if !isLogin(me) {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}

	if r.FormValue("csrf_token") != getCSRFToken(r) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		session := getSession(r)
		session.Values["notice"] = "画像が必須です"
		session.Save(r, w)

		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	mime := ""
	if file != nil {
		// 投稿のContent-Typeからファイルのタイプを決定する
		contentType := header.Header["Content-Type"][0]
		if strings.Contains(contentType, "jpeg") {
			mime = "image/jpeg"
		} else if strings.Contains(contentType, "png") {
			mime = "image/png"
		} else if strings.Contains(contentType, "gif") {
			mime = "image/gif"
		} else {
			session := getSession(r)
			session.Values["notice"] = "投稿できる画像形式はjpgとpngとgifだけです"
			session.Save(r, w)

			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
	}

	filedata, err := io.ReadAll(file)
	if err != nil {
		log.Print(err)
		return
	}

	if len(filedata) > UploadLimit {
		session := getSession(r)
		session.Values["notice"] = "ファイルサイズが大きすぎます"
		session.Save(r, w)

		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	query := "INSERT INTO `posts` (`user_id`, `mime`, `imgdata`, `body`) VALUES (?,?,?,?)"
	result, err := db.Exec(
		query,
		me.ID,
		mime,
		filedata,
		r.FormValue("body"),
	)
	if err != nil {
		log.Print(err)
		return
	}

	pid, err := result.LastInsertId()
	if err != nil {
		log.Print(err)
		return
	}

	// Cache the newly uploaded image
	if err := setImageCache(int(pid), filedata, mime); err != nil {
		log.Printf("Failed to cache uploaded image %d: %v", pid, err)
	}

	// Invalidate index posts cache since we added a new post
	memcacheConn.Delete("index_posts")

	http.Redirect(w, r, "/posts/"+strconv.FormatInt(pid, 10), http.StatusFound)
}

func getImage(w http.ResponseWriter, r *http.Request) {
	pidStr := r.PathValue("id")
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Try to get image from cache first
	imgdata, mime, err := getImageFromCache(pid)
	if err != nil {
		// Cache miss, fetch from database
		post := Post{}
		err = db.Get(&post, "SELECT * FROM `posts` WHERE `id` = ?", pid)
		if err != nil {
			log.Print(err)
			return
		}

		if post.ID == 0 {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		imgdata = post.Imgdata
		mime = post.Mime
		// Store in cache for next time
		if err := setImageCache(pid, imgdata, mime); err != nil {
			log.Printf("Failed to cache image %d: %v", pid, err)
		}
	}

	ext := r.PathValue("ext")

	if ext == "jpg" && mime == "image/jpeg" ||
		ext == "png" && mime == "image/png" ||
		ext == "gif" && mime == "image/gif" {
		w.Header().Set("Content-Type", mime)
		w.Header().Set("Cache-Control", "public, max-age=86400") // Cache for 1 day
		_, err := w.Write(imgdata)
		if err != nil {
			log.Print(err)
			return
		}
		return
	}

	w.WriteHeader(http.StatusNotFound)
}

func postComment(w http.ResponseWriter, r *http.Request) {
	me := getSessionUser(r)
	if !isLogin(me) {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}

	if r.FormValue("csrf_token") != getCSRFToken(r) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		return
	}

	postID, err := strconv.Atoi(r.FormValue("post_id"))
	if err != nil {
		log.Print("post_idは整数のみです")
		return
	}

	query := "INSERT INTO `comments` (`post_id`, `user_id`, `comment`) VALUES (?,?,?)"
	_, err = db.Exec(query, postID, me.ID, r.FormValue("comment"))
	if err != nil {
		log.Print(err)
		return
	}

	// Invalidate comment count cache when new comment is added
	key := getCacheKey("comment_count", postID)
	memcacheConn.Delete(key)

	// Invalidate user statistics cache for the commenter
	userStatsKey := getCacheKey("user_stats", me.ID)
	memcacheConn.Delete(userStatsKey)

	// Also invalidate the post owner's user statistics cache
	var postUserID int
	if err := db.Get(&postUserID, "SELECT user_id FROM posts WHERE id = ?", postID); err == nil {
		postOwnerStatsKey := getCacheKey("user_stats", postUserID)
		memcacheConn.Delete(postOwnerStatsKey)
	}

	// Invalidate post page cache for this specific post
	invalidatePostPageCache(postID)

	http.Redirect(w, r, fmt.Sprintf("/posts/%d", postID), http.StatusFound)
}

func getAdminBanned(w http.ResponseWriter, r *http.Request) {
	me := getSessionUser(r)
	if !isLogin(me) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	if me.Authority == 0 {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	users := []User{}
	err := db.Select(&users, "SELECT * FROM `users` WHERE `authority` = 0 AND `del_flg` = 0 ORDER BY `created_at` DESC")
	if err != nil {
		log.Print(err)
		return
	}

	template.Must(template.ParseFiles(
		getTemplPath("layout.html"),
		getTemplPath("banned.html")),
	).Execute(w, struct {
		Users     []User
		Me        User
		CSRFToken string
	}{users, me, getCSRFToken(r)})
}

func postAdminBanned(w http.ResponseWriter, r *http.Request) {
	me := getSessionUser(r)
	if !isLogin(me) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	if me.Authority == 0 {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if r.FormValue("csrf_token") != getCSRFToken(r) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		return
	}

	query := "UPDATE `users` SET `del_flg` = ? WHERE `id` = ?"

	err := r.ParseForm()
	if err != nil {
		log.Print(err)
		return
	}

	for _, id := range r.Form["uid[]"] {
		db.Exec(query, 1, id)
	}

	http.Redirect(w, r, "/admin/banned", http.StatusFound)
}

// Page-level caching for post detail pages
func getPostPageFromCache(postID int, userID int) (string, error) {
	key := fmt.Sprintf("post_page:%d:%d", postID, userID)
	item, err := memcacheConn.Get(key)
	if err != nil {
		return "", err
	}
	return string(item.Value), nil
}

func setPostPageCache(postID int, userID int, content string) error {
	key := fmt.Sprintf("post_page:%d:%d", postID, userID)
	return memcacheConn.Set(&memcache.Item{
		Key:        key,
		Value:      []byte(content),
		Expiration: 300, // 5 minutes for page cache
	})
}

func invalidatePostPageCache(postID int) {
	// More efficient approach: use a pattern-based key to track active user sessions
	// Instead of deleting 1000 potential keys, use a smaller range based on actual users

	// First, try to delete common user cache entries (logged in users typically have IDs 1-100)
	for i := 0; i <= 100; i++ {
		key := fmt.Sprintf("post_page:%d:%d", postID, i)
		memcacheConn.Delete(key)
	}

	// Also invalidate any guest user cache (userID = 0)
	guestKey := fmt.Sprintf("post_page:%d:0", postID)
	memcacheConn.Delete(guestKey)
}

// Enhanced user session caching
func getUserSessionFromCache(sessionID string) (*User, error) {
	key := getCacheKey("session", sessionID)
	item, err := memcacheConn.Get(key)
	if err != nil {
		return nil, err
	}

	var user User
	if err := json.Unmarshal(item.Value, &user); err != nil {
		return nil, err
	}
	return &user, nil
}

func setUserSessionCache(sessionID string, user *User) error {
	key := getCacheKey("session", sessionID)
	data, err := json.Marshal(user)
	if err != nil {
		return err
	}

	return memcacheConn.Set(&memcache.Item{
		Key:        key,
		Value:      data,
		Expiration: 1800, // 30 minutes for session cache
	})
}

// Batch operations for better cache efficiency
func batchGetUsersFromCache(userIDs []int) (map[int]*User, []int) {
	foundUsers := make(map[int]*User)
	missedIDs := []int{}

	// Prepare cache keys
	keys := make([]string, len(userIDs))
	for i, id := range userIDs {
		keys[i] = getCacheKey("user", id)
	}

	// Batch get from memcache
	items, err := memcacheConn.GetMulti(keys)
	if err != nil {
		// If batch fails, fall back to individual gets
		for _, id := range userIDs {
			if user, err := getUserFromCache(id); err == nil {
				foundUsers[id] = user
			} else {
				missedIDs = append(missedIDs, id)
			}
		}
		return foundUsers, missedIDs
	}

	// Process results
	for i, id := range userIDs {
		key := keys[i]
		if item, found := items[key]; found {
			var user User
			if err := json.Unmarshal(item.Value, &user); err == nil {
				foundUsers[id] = &user
			} else {
				missedIDs = append(missedIDs, id)
			}
		} else {
			missedIDs = append(missedIDs, id)
		}
	}

	return foundUsers, missedIDs
}

func batchSetUsersCache(users []User) {
	for _, user := range users {
		// Use goroutine for non-blocking cache writes
		go func(u User) {
			if err := setUserCache(&u); err != nil {
				log.Printf("Failed to cache user %d: %v", u.ID, err)
			}
		}(user)
	}
}

func main() {
	host := os.Getenv("ISUCONP_DB_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("ISUCONP_DB_PORT")
	if port == "" {
		port = "3306"
	}
	_, err := strconv.Atoi(port)
	if err != nil {
		log.Fatalf("Failed to read DB port number from an environment variable ISUCONP_DB_PORT.\nError: %s", err.Error())
	}
	user := os.Getenv("ISUCONP_DB_USER")
	if user == "" {
		user = "root"
	}
	password := os.Getenv("ISUCONP_DB_PASSWORD")
	dbname := os.Getenv("ISUCONP_DB_NAME")
	if dbname == "" {
		dbname = "isuconp"
	}

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		user,
		password,
		host,
		port,
		dbname,
	)

	db, err = sqlx.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %s.", err.Error())
	}
	defer db.Close()

	// Optimize database connection pool for high load
	db.SetMaxOpenConns(200)                 // Further increased for better throughput
	db.SetMaxIdleConns(75)                  // More idle connections
	db.SetConnMaxLifetime(20 * time.Minute) // Even shorter lifetime for better connection cycling
	db.SetConnMaxIdleTime(3 * time.Minute)  // Close idle connections faster

	r := chi.NewRouter()

	// Add response compression middleware for better performance (single instance)
	r.Use(middleware.Compress(5, "text/html", "text/css", "text/javascript", "application/javascript"))

	// Conditional logging only in development
	if os.Getenv("ISUCON_ENV") != "production" {
		r.Use(middleware.RequestID)
		r.Use(middleware.RealIP)
		r.Use(middleware.Logger)
	}

	r.Get("/initialize", getInitialize)
	r.Get("/login", getLogin)
	r.Post("/login", postLogin)
	r.Get("/register", getRegister)
	r.Post("/register", postRegister)
	r.Get("/logout", getLogout)
	r.Get("/", getIndex)
	r.Get("/posts", getPosts)
	r.Get("/posts/{id}", getPostsID)
	r.Post("/", postIndex)
	r.Get("/image/{id}.{ext}", getImage)
	r.Post("/comment", postComment)
	r.Get("/admin/banned", getAdminBanned)
	r.Post("/admin/banned", postAdminBanned)
	r.Get(`/@{accountName:[a-zA-Z]+}`, getAccountName)

	// Static file serving with proper cache headers
	r.Handle("/*", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set cache headers for static files
		if strings.HasSuffix(r.URL.Path, ".css") || strings.HasSuffix(r.URL.Path, ".js") ||
			strings.HasSuffix(r.URL.Path, ".png") || strings.HasSuffix(r.URL.Path, ".jpg") ||
			strings.HasSuffix(r.URL.Path, ".gif") || strings.HasSuffix(r.URL.Path, ".ico") {
			w.Header().Set("Cache-Control", "public, max-age=86400") // 1 day
		}
		http.FileServer(http.Dir("../public")).ServeHTTP(w, r)
	}))

	log.Fatal(http.ListenAndServe(":8080", r))
}

package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"html"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"path"
	"regexp"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/gorilla/sessions"
	"github.com/jmoiron/sqlx"
)

var (
	db    *sqlx.DB
	store = newMemorySessionStore()
)

const (
	postsPerPage  = 20
	ISO8601Format = "2006-01-02T15:04:05-07:00"
	UploadLimit   = 10 * 1024 * 1024 // 10mb

	// nginx が /image/ として直接配信するディレクトリ
	imageDir = "../public/image"
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
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

// セッションをアプリのメモリに持つ sessions.Store。Cookie にはランダムなセッション ID だけを入れる。
// memcached を使う gorilla-sessions-memcache では、通信・Cookie の署名検証（HMAC）・gob の変換で
// アプリの CPU の約2割を使っていた。アプリは1プロセスなので、メモリに持っても食い違わない
// （再起動するとログイン状態は消える）
type memorySessionStore struct {
	mu       sync.RWMutex
	sessions map[string]map[any]any
	options  sessions.Options
}

func newMemorySessionStore() *memorySessionStore {
	return &memorySessionStore{
		sessions: map[string]map[any]any{},
		// gorilla-sessions-memcache の既定値と同じ
		options: sessions.Options{Path: "/", MaxAge: 86400 * 30},
	}
}

// 同じリクエスト内では1回だけ New が呼ばれ、以降は同じ *sessions.Session が返る
func (s *memorySessionStore) Get(r *http.Request, name string) (*sessions.Session, error) {
	return sessions.GetRegistry(r).Get(s, name)
}

func (s *memorySessionStore) New(r *http.Request, name string) (*sessions.Session, error) {
	session := sessions.NewSession(s, name)
	opts := s.options
	session.Options = &opts
	session.IsNew = true

	c, err := r.Cookie(name)
	if err != nil {
		return session, nil
	}

	s.mu.RLock()
	values, ok := s.sessions[c.Value]
	s.mu.RUnlock()
	if !ok {
		return session, nil
	}

	// 同じユーザーの並行リクエストと map を共有しないよう、コピーして渡す
	for k, v := range values {
		session.Values[k] = v
	}
	session.ID = c.Value
	session.IsNew = false
	return session, nil
}

func (s *memorySessionStore) Save(r *http.Request, w http.ResponseWriter, session *sessions.Session) error {
	if session.Options.MaxAge < 0 {
		if session.ID != "" {
			s.mu.Lock()
			delete(s.sessions, session.ID)
			s.mu.Unlock()
		}
		http.SetCookie(w, sessions.NewCookie(session.Name(), "", session.Options))
		return nil
	}

	if session.ID == "" {
		session.ID = secureRandomStr(16)
	}
	values := make(map[any]any, len(session.Values))
	for k, v := range session.Values {
		values[k] = v
	}

	s.mu.Lock()
	s.sessions[session.ID] = values
	s.mu.Unlock()

	http.SetCookie(w, sessions.NewCookie(session.Name(), session.ID, session.Options))
	return nil
}

// /initialize で呼ぶ。ベンチマークのたびにセッションが溜まり続けないようにする
func (s *memorySessionStore) clear() {
	s.mu.Lock()
	s.sessions = map[string]map[any]any{}
	s.mu.Unlock()
}

func dbInitialize(ctx context.Context) {
	sqls := []string{
		"DELETE FROM users WHERE id > 1000",
		"DELETE FROM posts WHERE id > 10000",
		"DELETE FROM comments WHERE id > 100000",
		"UPDATE users SET del_flg = 0",
		"UPDATE users SET del_flg = 1 WHERE id % 50 = 0",
	}

	for _, sql := range sqls {
		db.ExecContext(ctx, sql)
	}

	createIndexes(ctx)
	removeAddedImages()
	store.clear()

	if err := userCache.load(ctx); err != nil {
		log.Print(err)
	}
	if err := commentCache.load(ctx); err != nil {
		log.Print(err)
	}
	if err := postCache.load(ctx); err != nil {
		log.Print(err)
	}
}

// users の全件をメモリに持つキャッシュ。users は小さく、変更されるのは
// 登録（postRegister）・BAN（postAdminBanned）・/initialize だけなので、そこで更新する
type usersCache struct {
	mu     sync.RWMutex
	byID   map[int]User
	byName map[string]int
}

var userCache = &usersCache{}

func (c *usersCache) load(ctx context.Context) error {
	var us []User
	if err := db.SelectContext(ctx, &us, "SELECT * FROM `users`"); err != nil {
		return err
	}

	byID := make(map[int]User, len(us))
	byName := make(map[string]int, len(us))
	for _, u := range us {
		byID[u.ID] = u
		byName[u.AccountName] = u.ID
	}

	c.mu.Lock()
	c.byID, c.byName = byID, byName
	c.mu.Unlock()
	return nil
}

func (c *usersCache) get(id int) (User, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	u, ok := c.byID[id]
	return u, ok
}

func (c *usersCache) getByName(accountName string) (User, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	id, ok := c.byName[accountName]
	if !ok {
		return User{}, false
	}
	return c.byID[id], true
}

func (c *usersCache) set(u User) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byID[u.ID] = u
	c.byName[u.AccountName] = u.ID
}

func (c *usersCache) ban(ids []int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range ids {
		if u, ok := c.byID[id]; ok {
			u.DelFlg = 1
			c.byID[id] = u
		}
	}
}

// comments の全件を投稿ごとにメモリに持つキャッシュ。一覧の「件数と最新3件」を取るクエリが
// MySQL の処理時間の約3割を占めていた。変更されるのは postComment と /initialize だけなので、そこで更新する
type commentsCache struct {
	mu          sync.RWMutex
	byPost      map[int][]Comment // 投稿ごとに古い順（created_at, id の昇順）
	countByUser map[int]int       // ユーザーごとのコメント数
}

var commentCache = &commentsCache{}

func (c *commentsCache) load(ctx context.Context) error {
	var cs []Comment
	if err := db.SelectContext(ctx, &cs, "SELECT `id`, `post_id`, `user_id`, `comment`, `created_at` FROM `comments` ORDER BY `created_at`, `id`"); err != nil {
		return err
	}

	byPost := make(map[int][]Comment)
	countByUser := make(map[int]int)
	for _, cm := range cs {
		byPost[cm.PostID] = append(byPost[cm.PostID], cm)
		countByUser[cm.UserID]++
	}

	c.mu.Lock()
	c.byPost, c.countByUser = byPost, countByUser
	c.mu.Unlock()
	return nil
}

func (c *commentsCache) add(cm Comment) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.countByUser[cm.UserID]++

	// 同時に投稿されると INSERT と逆の順で届くことがあるので、並び順を保つ位置に入れる
	cs := c.byPost[cm.PostID]
	i := len(cs)
	for i > 0 && (cs[i-1].CreatedAt.After(cm.CreatedAt) ||
		cs[i-1].CreatedAt.Equal(cm.CreatedAt) && cs[i-1].ID > cm.ID) {
		i--
	}
	cs = append(cs, Comment{})
	copy(cs[i+1:], cs[i:])
	cs[i] = cm
	c.byPost[cm.PostID] = cs
}

// コメント数と、古い順のコメント（limit > 0 なら最新 limit 件）を返す。
// 返すスライスはコピーなので、呼び出し側で書き換えてよい
func (c *commentsCache) get(postID int, limit int) (int, []Comment) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	cs := c.byPost[postID]
	n := len(cs)
	if limit > 0 && n > limit {
		cs = cs[n-limit:]
	}
	return n, append([]Comment(nil), cs...)
}

// ユーザーが書いたコメントの数
func (c *commentsCache) countByUserID(userID int) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.countByUser[userID]
}

// 指定した投稿についたコメントの合計数
func (c *commentsCache) countOnPosts(postIDs []int) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n := 0
	for _, id := range postIDs {
		n += len(c.byPost[id])
	}
	return n
}

// posts（画像を除く）の全件をメモリに持つキャッシュ。タイムライン・投稿詳細・ユーザーページを
// DB に問い合わせずに返すため。変更されるのは postIndex と /initialize だけなので、そこで更新する
type postsCache struct {
	mu     sync.RWMutex
	byID   map[int]Post
	sorted []int         // 投稿 ID を created_at, id の昇順に並べたもの
	byUser map[int][]int // ユーザーごとの投稿 ID（並びは sorted と同じ）
}

var postCache = &postsCache{}

func (c *postsCache) load(ctx context.Context) error {
	var ps []Post
	if err := db.SelectContext(ctx, &ps, "SELECT `id`, `user_id`, `body`, `mime`, `created_at` FROM `posts` ORDER BY `created_at`, `id`"); err != nil {
		return err
	}

	byID := make(map[int]Post, len(ps))
	sorted := make([]int, 0, len(ps))
	byUser := make(map[int][]int)
	for _, p := range ps {
		byID[p.ID] = p
		sorted = append(sorted, p.ID)
		byUser[p.UserID] = append(byUser[p.UserID], p.ID)
	}

	c.mu.Lock()
	c.byID, c.sorted, c.byUser = byID, sorted, byUser
	c.mu.Unlock()
	return nil
}

// a が b より古いか（created_at, id の順で比べる）
func (c *postsCache) older(a, b int) bool {
	pa, pb := c.byID[a], c.byID[b]
	if pa.CreatedAt.Equal(pb.CreatedAt) {
		return pa.ID < pb.ID
	}
	return pa.CreatedAt.Before(pb.CreatedAt)
}

// 並び順を保つ位置に id を入れる。新しい投稿はほぼ末尾に入る
func (c *postsCache) insertSorted(ids []int, id int) []int {
	i := len(ids)
	for i > 0 && c.older(id, ids[i-1]) {
		i--
	}
	ids = append(ids, 0)
	copy(ids[i+1:], ids[i:])
	ids[i] = id
	return ids
}

func (c *postsCache) add(p Post) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.byID[p.ID] = p
	c.sorted = c.insertSorted(c.sorted, p.ID)
	c.byUser[p.UserID] = c.insertSorted(c.byUser[p.UserID], p.ID)
}

func (c *postsCache) get(id int) (Post, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	p, ok := c.byID[id]
	return p, ok
}

// created_at が maxCreatedAt 以下の投稿を新しい順に n 件返す（削除済みユーザーの投稿は除く）。
// maxCreatedAt がゼロ値なら最新から返す
func (c *postsCache) timeline(maxCreatedAt time.Time, n int) []Post {
	c.mu.RLock()
	defer c.mu.RUnlock()

	end := len(c.sorted)
	if !maxCreatedAt.IsZero() {
		end = sort.Search(len(c.sorted), func(i int) bool {
			return c.byID[c.sorted[i]].CreatedAt.After(maxCreatedAt)
		})
	}

	posts := make([]Post, 0, n)
	for i := end - 1; i >= 0 && len(posts) < n; i-- {
		p := c.byID[c.sorted[i]]
		if u, ok := userCache.get(p.UserID); ok && u.DelFlg == 0 {
			posts = append(posts, p)
		}
	}
	return posts
}

// ユーザーの投稿を新しい順に n 件と、そのユーザーの全投稿 ID を返す
func (c *postsCache) byUserID(userID int, n int) ([]Post, []int) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ids := c.byUser[userID]
	posts := make([]Post, 0, n)
	for i := len(ids) - 1; i >= 0 && len(posts) < n; i-- {
		posts = append(posts, c.byID[ids[i]])
	}
	return posts, append([]int(nil), ids...)
}

// ベンチマーク中に投稿された画像（id > 10000）のファイルを削除する
func removeAddedImages() {
	entries, err := os.ReadDir(imageDir)
	if err != nil {
		log.Print(err)
		return
	}

	for _, e := range entries {
		id, err := strconv.Atoi(strings.SplitN(e.Name(), ".", 2)[0])
		if err != nil || id <= 10000 {
			continue
		}
		if err := os.Remove(path.Join(imageDir, e.Name())); err != nil {
			log.Print(err)
		}
	}
}

// インデックスが無ければ作成する（2回目以降の /initialize では何もしない）
func createIndexes(ctx context.Context) {
	indexes := []struct {
		table   string
		name    string
		columns string
	}{
		{"comments", "post_id_idx", "`post_id`, `created_at`"},
		{"comments", "user_id_idx", "`user_id`"},
		{"posts", "created_at_idx", "`created_at`"},
		{"posts", "user_id_idx", "`user_id`, `created_at`"},
	}

	for _, idx := range indexes {
		var count int
		err := db.GetContext(ctx, &count,
			"SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?",
			idx.table, idx.name)
		if err != nil {
			log.Print(err)
			continue
		}
		if count > 0 {
			continue
		}

		_, err = db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE `%s` ADD INDEX `%s` (%s)", idx.table, idx.name, idx.columns))
		if err != nil {
			log.Print(err)
		}
	}
}

func tryLogin(accountName, password string) *User {
	u, ok := userCache.getByName(accountName)
	if !ok || u.DelFlg != 0 {
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

// openssl dgst -sha512 と同じ結果（小文字の16進数）を、外部コマンドを使わずに計算する
func digest(src string) string {
	sum := sha512.Sum512([]byte(src))
	return hex.EncodeToString(sum[:])
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
	ctx := r.Context()
	session := getSession(r)
	// ログイン時は int、登録時は LastInsertId の int64 が入っている
	var uid int
	switch v := session.Values["user_id"].(type) {
	case int:
		uid = v
	case int64:
		uid = int(v)
	default:
		return User{}
	}

	users, err := getUsersByIDs(ctx, []int{uid})
	if err != nil {
		return User{}
	}

	return users[uid]
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

// キャッシュから引き、キャッシュに無いものだけ DB から取る
func getUsersByIDs(ctx context.Context, ids []int) (map[int]User, error) {
	users := make(map[int]User, len(ids))
	var missing []int
	for _, id := range ids {
		if u, ok := userCache.get(id); ok {
			users[id] = u
		} else {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return users, nil
	}

	query, args, err := sqlx.In("SELECT * FROM `users` WHERE `id` IN (?)", missing)
	if err != nil {
		return nil, err
	}
	var us []User
	if err := db.SelectContext(ctx, &us, query, args...); err != nil {
		return nil, err
	}

	for _, u := range us {
		users[u.ID] = u
		userCache.set(u)
	}
	return users, nil
}

// ユーザーとコメントはキャッシュから取るので、DB には問い合わせない
func makePosts(ctx context.Context, results []Post, csrfToken string, allComments bool) ([]Post, error) {
	var posts []Post
	if len(results) == 0 {
		return posts, nil
	}

	// 投稿者を取得し、削除済みユーザーの投稿を除いて postsPerPage 件に絞る
	userIDs := make([]int, 0, len(results))
	for _, p := range results {
		userIDs = append(userIDs, p.UserID)
	}
	postUsers, err := getUsersByIDs(ctx, userIDs)
	if err != nil {
		return nil, err
	}

	for _, p := range results {
		p.User = postUsers[p.UserID]
		p.CSRFToken = csrfToken

		if p.User.DelFlg == 0 {
			posts = append(posts, p)
		}
		if len(posts) >= postsPerPage {
			break
		}
	}
	if len(posts) == 0 {
		return posts, nil
	}

	// コメント数とコメント（古い順）はキャッシュから取る。一覧では投稿ごとに最新3件だけ
	limit := 3
	if allComments {
		limit = 0
	}
	userIDs = userIDs[:0]
	for i := range posts {
		posts[i].CommentCount, posts[i].Comments = commentCache.get(posts[i].ID, limit)
		for _, c := range posts[i].Comments {
			userIDs = append(userIDs, c.UserID)
		}
	}

	// コメントしたユーザー
	commentUsers, err := getUsersByIDs(ctx, userIDs)
	if err != nil {
		return nil, err
	}
	for i := range posts {
		for j := range posts[i].Comments {
			posts[i].Comments[j].User = commentUsers[posts[i].Comments[j].UserID]
		}
	}

	return posts, nil
}

func imageExt(mime string) string {
	if mime == "image/jpeg" {
		return ".jpg"
	} else if mime == "image/png" {
		return ".png"
	} else if mime == "image/gif" {
		return ".gif"
	}
	return ""
}

func imageURL(p Post) string {
	return "/image/" + strconv.Itoa(p.ID) + imageExt(p.Mime)
}

// 投稿1件分の HTML を組み立てる（元は templates/post.html）。
// 一覧では投稿・コメントの数だけ繰り返し描画するので、html/template のリフレクションと
// 文脈ごとのエスケープが CPU の2割以上を占めていた。値はすべて html.EscapeString でエスケープする
func writePost(b *bytes.Buffer, p Post) {
	id := strconv.Itoa(p.ID)
	createdAt := p.CreatedAt.Format(ISO8601Format)
	accountName := html.EscapeString(p.User.AccountName)

	b.WriteString(`<div class="isu-post" id="pid_`)
	b.WriteString(id)
	b.WriteString(`" data-created-at="`)
	b.WriteString(createdAt)
	b.WriteString(`">
  <div class="isu-post-header">
    <a href="/@`)
	b.WriteString(accountName)
	b.WriteString(` " class="isu-post-account-name">`)
	b.WriteString(accountName)
	b.WriteString(`</a>
    <a href="/posts/`)
	b.WriteString(id)
	b.WriteString(`" class="isu-post-permalink">
      <time class="timeago" datetime="`)
	b.WriteString(createdAt)
	b.WriteString(`"></time>
    </a>
  </div>
  <div class="isu-post-image">
    <img src="`)
	b.WriteString(imageURL(p))
	b.WriteString(`" class="isu-image">
  </div>
  <div class="isu-post-text">
    <a href="/@`)
	b.WriteString(accountName)
	b.WriteString(`" class="isu-post-account-name">`)
	b.WriteString(accountName)
	b.WriteString(`</a>
    `)
	b.WriteString(html.EscapeString(p.Body))
	b.WriteString(`
  </div>
  <div class="isu-post-comment">
    <div class="isu-post-comment-count">
      comments: <b>`)
	b.WriteString(strconv.Itoa(p.CommentCount))
	b.WriteString(`</b>
    </div>
`)
	for _, c := range p.Comments {
		commenter := html.EscapeString(c.User.AccountName)
		b.WriteString(`    <div class="isu-comment">
      <a href="/@`)
		b.WriteString(commenter)
		b.WriteString(`" class="isu-comment-account-name">`)
		b.WriteString(commenter)
		b.WriteString(`</a>
      <span class="isu-comment-text">`)
		b.WriteString(html.EscapeString(c.Comment))
		b.WriteString(`</span>
    </div>
`)
	}
	b.WriteString(`    <div class="isu-comment-form">
      <form method="post" action="/comment">
        <input type="text" name="comment">
        <input type="hidden" name="post_id" value="`)
	b.WriteString(id)
	b.WriteString(`">
        <input type="hidden" name="csrf_token" value="`)
	b.WriteString(html.EscapeString(p.CSRFToken))
	b.WriteString(`">
        <input type="submit" name="submit" value="submit">
      </form>
    </div>
  </div>
</div>
`)
}

// ページの HTML を組み立てるバッファを使い回す（元は templates/*.html を html/template で描画していた。
// html/template のリフレクションと文脈ごとのエスケープが CPU の1割強を占めていたので、すべて手書きにした）
var bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

// 組み立てた HTML を1回の write で送る
func writeHTML(w http.ResponseWriter, b *bytes.Buffer) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(b.Bytes())
	b.Reset()
	bufPool.Put(b)
}

// layout.html の前半（ヘッダーまで）
func writeLayoutHeader(b *bytes.Buffer, me User) {
	b.WriteString(`<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Iscogram</title>
    <link href="/css/style.css" media="screen" rel="stylesheet" type="text/css">
  </head>
  <body>
    <div class="container">
      <div class="header">
        <div class="isu-title">
          <h1><a href="/">Iscogram</a></h1>
        </div>
        <div class="isu-header-menu">
`)
	if me.ID == 0 {
		b.WriteString(`          <div><a href="/login">ログイン</a></div>
`)
	} else {
		name := html.EscapeString(me.AccountName)
		b.WriteString(`          <div><a href="/@`)
		b.WriteString(name)
		b.WriteString(`"><span class="isu-account-name">`)
		b.WriteString(name)
		b.WriteString(`</span>さん</a></div>
`)
		if me.Authority == 1 {
			b.WriteString(`          <div><a href="/admin/banned">管理者用ページ</a></div>
`)
		}
		b.WriteString(`          <div><a href="/logout">ログアウト</a></div>
`)
	}
	b.WriteString(`        </div>
      </div>

`)
}

// layout.html の後半
func writeLayoutFooter(b *bytes.Buffer) {
	b.WriteString(`
    </div>
    <script src="/js/timeago.min.js"></script>
    <script src="/js/main.js"></script>
  </body>
</html>
`)
}

func writeFlash(b *bytes.Buffer, flash string) {
	if flash == "" {
		return
	}
	b.WriteString(`<div id="notice-message" class="alert alert-danger">
  `)
	b.WriteString(html.EscapeString(flash))
	b.WriteString(`
</div>
`)
}

// posts.html
func writePosts(b *bytes.Buffer, posts []Post) {
	b.WriteString(`<div class="isu-posts">
`)
	for _, p := range posts {
		writePost(b, p)
	}
	b.WriteString(`</div>
`)
}

// login.html と register.html（見出しとフォームの送り先だけが違う）
func writeAccountForm(b *bytes.Buffer, me User, title, action, flash string, registerLink bool) {
	writeLayoutHeader(b, me)
	b.WriteString(`<div class="header">
  <h1>`)
	b.WriteString(title)
	b.WriteString(`</h1>
</div>

`)
	writeFlash(b, flash)
	b.WriteString(`
<div class="submit">
  <form method="post" action="`)
	b.WriteString(action)
	b.WriteString(`">
    <div class="form-account-name">
      <span>アカウント名</span>
      <input type="text" name="account_name">
    </div>
    <div class="form-password">
      <span>パスワード</span>
      <input type="password" name="password">
    </div>
    <div class="form-submit">
      <input type="submit" name="submit" value="submit">
    </div>
  </form>
</div>
`)
	if registerLink {
		b.WriteString(`
<div class="isu-register">
  <a href="/register">ユーザー登録</a>
</div>
`)
	}
	writeLayoutFooter(b)
}

// 画像をファイルに書き出す。nginx が書きかけのファイルを返さないよう、一時ファイルに書いてから rename する
func writeImageFile(id int, mime string, data []byte) error {
	tmp, err := os.CreateTemp(imageDir, ".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp.Name(), 0644); err != nil {
		return err
	}

	return os.Rename(tmp.Name(), path.Join(imageDir, strconv.Itoa(id)+imageExt(mime)))
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

func getInitialize(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dbInitialize(ctx)
	w.WriteHeader(http.StatusOK)
}

func getLogin(w http.ResponseWriter, r *http.Request) {
	me := getSessionUser(r)

	if isLogin(me) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	flash := getFlash(w, r, "notice")
	b := bufPool.Get().(*bytes.Buffer)
	writeAccountForm(b, me, "ログイン", "/login", flash, true)
	writeHTML(w, b)
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

	flash := getFlash(w, r, "notice")
	b := bufPool.Get().(*bytes.Buffer)
	writeAccountForm(b, User{}, "ユーザー登録", "/register", flash, false)
	writeHTML(w, b)
}

func postRegister(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
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

	if _, exists := userCache.getByName(accountName); exists {
		session := getSession(r)
		session.Values["notice"] = "アカウント名がすでに使われています"
		session.Save(r, w)

		http.Redirect(w, r, "/register", http.StatusFound)
		return
	}

	query := "INSERT INTO `users` (`account_name`, `passhash`) VALUES (?,?)"
	result, err := db.ExecContext(ctx, query, accountName, calculatePasshash(accountName, password))
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

	// created_at などの既定値を含めてキャッシュに入れるため、登録した行を読み直す
	u := User{}
	if err := db.GetContext(ctx, &u, "SELECT * FROM `users` WHERE `id` = ?", uid); err != nil {
		log.Print(err)
		return
	}
	userCache.set(u)

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
	ctx := r.Context()
	me := getSessionUser(r)

	results := postCache.timeline(time.Time{}, postsPerPage)

	posts, err := makePosts(ctx, results, getCSRFToken(r), false)
	if err != nil {
		log.Print(err)
		return
	}

	csrfToken := getCSRFToken(r)
	// getFlash は Cookie を書き換えるので、本文を書き始める前に呼ぶ
	flash := getFlash(w, r, "notice")

	// index.html
	b := bufPool.Get().(*bytes.Buffer)
	writeLayoutHeader(b, me)
	b.WriteString(`<div class="isu-submit">
  <form method="post" action="/" enctype="multipart/form-data">
    <div class="isu-form">
      <input type="file" name="file" value="file">
    </div>
    <div class="isu-form">
      <textarea name="body"></textarea>
    </div>
    <div class="form-submit">
      <input type="hidden" name="csrf_token" value="`)
	b.WriteString(html.EscapeString(csrfToken))
	b.WriteString(`">
      <input type="submit" name="submit" value="submit">
    </div>
`)
	if flash != "" {
		b.WriteString(`    <div id="notice-message" class="alert alert-danger">
      `)
		b.WriteString(html.EscapeString(flash))
		b.WriteString(`
    </div>
`)
	}
	b.WriteString(`  </form>
</div>

`)
	writePosts(b, posts)
	b.WriteString(`
<div id="isu-post-more">
  <button id="isu-post-more-btn">もっと見る</button>
  <img class="isu-loading-icon" src="/img/ajax-loader.gif">
</div>
`)
	writeLayoutFooter(b)
	writeHTML(w, b)
}

func getAccountName(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	accountName := r.PathValue("accountName")

	user, ok := userCache.getByName(accountName)
	if !ok || user.DelFlg != 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// 投稿者は del_flg = 0 を確認済みなので、makePosts で絞られる件数を最初から取る
	results, postIDs := postCache.byUserID(user.ID, postsPerPage)

	posts, err := makePosts(ctx, results, getCSRFToken(r), false)
	if err != nil {
		log.Print(err)
		return
	}

	commentCount := commentCache.countByUserID(user.ID)
	postCount := len(postIDs)
	commentedCount := commentCache.countOnPosts(postIDs)

	me := getSessionUser(r)

	// user.html
	b := bufPool.Get().(*bytes.Buffer)
	writeLayoutHeader(b, me)
	b.WriteString(`<div class="isu-user">
  <div><span class="isu-user-account-name">`)
	b.WriteString(html.EscapeString(user.AccountName))
	b.WriteString(`さん</span>のページ</div>
  <div>投稿数 <span class="isu-post-count">`)
	b.WriteString(strconv.Itoa(postCount))
	b.WriteString(`</span></div>
  <div>コメント数 <span class="isu-comment-count">`)
	b.WriteString(strconv.Itoa(commentCount))
	b.WriteString(`</span></div>
  <div>被コメント数 <span class="isu-commented-count">`)
	b.WriteString(strconv.Itoa(commentedCount))
	b.WriteString(`</span></div>
</div>

`)
	writePosts(b, posts)
	writeLayoutFooter(b)
	writeHTML(w, b)
}

func getPosts(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
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

	results := postCache.timeline(t, postsPerPage)

	posts, err := makePosts(ctx, results, getCSRFToken(r), false)
	if err != nil {
		log.Print(err)
		return
	}

	if len(posts) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	b := bufPool.Get().(*bytes.Buffer)
	writePosts(b, posts)
	writeHTML(w, b)
}

func getPostsID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pidStr := r.PathValue("id")
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	p, ok := postCache.get(pid)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	posts, err := makePosts(ctx, []Post{p}, getCSRFToken(r), true)
	if err != nil {
		log.Print(err)
		return
	}

	if len(posts) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	me := getSessionUser(r)

	// post_id.html
	b := bufPool.Get().(*bytes.Buffer)
	writeLayoutHeader(b, me)
	writePost(b, posts[0])
	writeLayoutFooter(b)
	writeHTML(w, b)
}

func postIndex(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
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

	// 画像はファイルにだけ保存し、DB には入れない（imgdata は NOT NULL なので空にする）。
	// 画像を入れると、送信とエスケープで INSERT 1回に 30ms 以上かかっていた
	// 投稿は最後に postCache に入れた時点でタイムラインに載るので、画像ファイルは必ずそれより先にできている。
	// ファイルを書き出せなかったときに投稿ごと取り消せるよう、トランザクションの中で書き出す
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		log.Print(err)
		return
	}
	defer tx.Rollback()

	query := "INSERT INTO `posts` (`user_id`, `mime`, `imgdata`, `body`) VALUES (?,?,'',?)"
	result, err := tx.ExecContext(
		ctx,
		query,
		me.ID,
		mime,
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

	// 画像の保存先はファイルだけなので、書き出せなければ投稿を取り消す
	if err := writeImageFile(int(pid), mime, filedata); err != nil {
		log.Print(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// created_at は DB の既定値なので、登録した行を読み直してキャッシュに入れる
	p := Post{}
	if err := tx.GetContext(ctx, &p, "SELECT `id`, `user_id`, `body`, `mime`, `created_at` FROM `posts` WHERE `id` = ?", pid); err != nil {
		log.Print(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Print(err)
		os.Remove(path.Join(imageDir, strconv.FormatInt(pid, 10)+imageExt(mime)))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	postCache.add(p)

	http.Redirect(w, r, "/posts/"+strconv.FormatInt(pid, 10), http.StatusFound)
}

func getImage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pidStr := r.PathValue("id")
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	post := Post{}
	err = db.GetContext(ctx, &post, "SELECT * FROM `posts` WHERE `id` = ?", pid)
	if err != nil {
		log.Print(err)
		return
	}

	// 投稿された画像は DB に入れていない（ファイルは nginx が返す）。初期データの画像だけがここに来る
	if len(post.Imgdata) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	ext := r.PathValue("ext")

	if ext == "jpg" && post.Mime == "image/jpeg" ||
		ext == "png" && post.Mime == "image/png" ||
		ext == "gif" && post.Mime == "image/gif" {
		// 次回からは nginx がファイルを直接返す
		if err := writeImageFile(post.ID, post.Mime, post.Imgdata); err != nil {
			log.Print(err)
		}

		w.Header().Set("Content-Type", post.Mime)
		_, err := w.Write(post.Imgdata)
		if err != nil {
			log.Print(err)
			return
		}
		return
	}

	w.WriteHeader(http.StatusNotFound)
}

func postComment(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
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
	result, err := db.ExecContext(ctx, query, postID, me.ID, r.FormValue("comment"))
	if err != nil {
		log.Print(err)
		return
	}

	cid, err := result.LastInsertId()
	if err != nil {
		log.Print(err)
		return
	}

	// created_at は DB の既定値なので、登録した行を読み直してキャッシュに入れる
	c := Comment{}
	if err := db.GetContext(ctx, &c, "SELECT `id`, `post_id`, `user_id`, `comment`, `created_at` FROM `comments` WHERE `id` = ?", cid); err != nil {
		log.Print(err)
		return
	}
	commentCache.add(c)

	http.Redirect(w, r, fmt.Sprintf("/posts/%d", postID), http.StatusFound)
}

func getAdminBanned(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
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
	err := db.SelectContext(ctx, &users, "SELECT * FROM `users` WHERE `authority` = 0 AND `del_flg` = 0 ORDER BY `created_at` DESC")
	if err != nil {
		log.Print(err)
		return
	}

	// banned.html
	b := bufPool.Get().(*bytes.Buffer)
	writeLayoutHeader(b, me)
	b.WriteString(`<div>
  <form method="post" action="/admin/banned">
`)
	for _, u := range users {
		id := strconv.Itoa(u.ID)
		name := html.EscapeString(u.AccountName)
		b.WriteString(`    <div>
      <input type="checkbox" name="uid[]" id="uid_`)
		b.WriteString(id)
		b.WriteString(`" value="`)
		b.WriteString(id)
		b.WriteString(`" data-account-name="`)
		b.WriteString(name)
		b.WriteString(`"> <label for="uid_`)
		b.WriteString(id)
		b.WriteString(`">`)
		b.WriteString(name)
		b.WriteString(`</label>
    </div>
`)
	}
	b.WriteString(`    <div class="form-submit">
      <input type="hidden" name="csrf_token" value="`)
	b.WriteString(html.EscapeString(getCSRFToken(r)))
	b.WriteString(`">
      <input type="submit" name="submit" value="submit">
    </div>
  </form>
</div>
`)
	writeLayoutFooter(b)
	writeHTML(w, b)
}

func postAdminBanned(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
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

	err := r.ParseForm()
	if err != nil {
		log.Print(err)
		return
	}

	ids := make([]int, 0, len(r.Form["uid[]"]))
	for _, s := range r.Form["uid[]"] {
		id, err := strconv.Atoi(s)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}

	if len(ids) > 0 {
		query, args, err := sqlx.In("UPDATE `users` SET `del_flg` = 1 WHERE `id` IN (?)", ids)
		if err != nil {
			log.Print(err)
			return
		}
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			log.Print(err)
			return
		}
		userCache.ban(ids)
	}

	http.Redirect(w, r, "/admin/banned", http.StatusFound)
}

func main() {
	// GC が CPU の 17% を使っていたので頻度を下げる（既定の GOGC は 100）。
	// ヒープが 300MiB に近づいたら GOGC によらず GC する。コンテナの上限 1GiB には、投稿画像を
	// 書き出したページキャッシュも含まれる。600MiB にしたらメモリの回収が頻発してスコアが2割下がった
	debug.SetGCPercent(400)
	debug.SetMemoryLimit(300 << 20)

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

	cfg := mysql.NewConfig()
	cfg.User = user
	cfg.Passwd = password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%s", host, port)
	cfg.DBName = dbname
	cfg.Params = map[string]string{
		"charset": "utf8mb4",
	}
	cfg.ParseTime = true
	cfg.Loc = time.Local
	// プレースホルダをクライアント側で展開し、PREPARE の往復をなくす
	cfg.InterpolateParams = true
	dsn := cfg.FormatDSN()

	db, err = sqlx.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %s.", err.Error())
	}
	defer db.Close()

	// 既定では待機接続が2本までで、同時リクエストが増えると接続・切断を繰り返すので広げる
	db.SetMaxOpenConns(64)
	db.SetMaxIdleConns(64)
	db.SetConnMaxLifetime(time.Minute)

	// MySQL より先に起動することがあるので、つながるまで待ってから読み込む
	for i := 0; ; i++ {
		err := userCache.load(context.Background())
		if err == nil {
			err = commentCache.load(context.Background())
		}
		if err == nil {
			err = postCache.load(context.Background())
		}
		if err == nil {
			break
		}
		if i >= 60 {
			log.Fatalf("Failed to load caches: %s.", err.Error())
		}
		log.Print(err)
		time.Sleep(time.Second)
	}

	// pprof（計測用）。nginx を通さず、Docker ネットワーク内から app:6060 で取得する
	go func() {
		log.Print(http.ListenAndServe(":6060", nil))
	}()

	r := chi.NewRouter()

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
	r.Get(`/@{accountName:[0-9a-zA-Z_]+}`, getAccountName)
	r.Mount("/", http.FileServer(http.Dir("../public")))

	log.Fatal(http.ListenAndServe(":8080", r))
}

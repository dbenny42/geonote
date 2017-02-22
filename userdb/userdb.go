package userdb

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"database/sql"
	"math/rand"

	"golang.org/x/crypto/bcrypt"
	_ "github.com/go-sql-driver/mysql"
)

const (
	MAX_USERNAME_LEN = 124
	SALT_LEN = 32
	HASH_LEN = 60
	SALT_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_!@#$%^&*()"
)

type UserdbConnection interface {
	RegisterUser(username string, password string) error
	DeleteUser(username string) error
	CheckCredentials(username string, password string) (bool, error)
}

type UserEntry struct {
	Name string
	Salt string
	Hash []byte
}

type MysqlUserdb struct {
	conn *sql.DB
}

type DbCredentials struct {
	User string
	Password string
	Host string
	Port string
}

func NewMysqlUserdb(credentials *DbCredentials) (*MysqlUserdb, error) {
	dsn := credentials.User + ":" + credentials.Password + "@tcp(" + 
		credentials.Host + ":" + credentials.Port + ")/geonote?parseTime=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Print("Failed to open db:", err)
		return nil, err
	}

	return &MysqlUserdb{conn: db}, nil
}

func (db MysqlUserdb) RegisterUser(username string, password string) error {
	userEntry, err := createUserEntry(username, password)
	if err != nil {
		log.Printf("Failed to make user entry with name: %v", username)
	}

	insertSql := "INSERT INTO users " + 
		" (name, salt, hash) VALUES " +
		" (?, ?, ?) "

	statement, err := db.conn.Prepare(insertSql)
	if err != nil {
		log.Printf("Failed to prepare statement %v. Err: %v", insertSql, err)
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(
		userEntry.Name,
		userEntry.Salt,
		string(userEntry.Hash[:HASH_LEN]),
	)
	if err != nil {
		log.Printf("Failed to register user. Err:", err)
		return err
	}

	return nil
}

func (db MysqlUserdb) DeleteUser(username string) error {
	sql := "DELETE from users WHERE name = ?"
	statement, err := db.conn.Prepare(sql)
	if err != nil {
		log.Printf("Failed to prepare statement %v. Err: %v", sql, err)
		return err
	}
	defer statement.Close()

	result, err := statement.Exec(username)
	if err != nil {
		log.Printf("Failed to delete user: %v", username)
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Printf("Error while fetching rows affected during delete. Err: %v", err)
		return err
	}
	
	if rowsAffected != 1 {
		msg := fmt.Sprintf("Delete user result is incorrect; actually affected %v entries while deleting %v.", rowsAffected, username)
		log.Printf(msg)
		return errors.New(msg)
	}

	return nil
}

func (db MysqlUserdb) CheckCredentials(username string, password string) (bool, error) {
	userEntry, err := getUserEntry(db, username)
	if err != nil {
		return false, err
	}
	
	possibleHash, err := getHash(password, userEntry.Salt)
	if err != nil {
		return false, err
	}
	
	correctLogin := bytes.Equal(possibleHash, userEntry.Hash)
	return correctLogin, nil
}

func getHash(password string, salt string) ([]byte, error) {
	passSalt := password + salt
	return bcrypt.GenerateFromPassword([]byte(passSalt), bcrypt.DefaultCost)
}

func getUserEntry(db MysqlUserdb, username string) (*UserEntry, error) {
	sql := "SELECT name, salt, hash from users where name = ?"
	statement, err := db.conn.Prepare(sql)
	if err != nil {
		log.Printf("Failed to prepare statement %v. Err: %v", sql, err)
		return nil, err
	}
	defer statement.Close()

	rows, err := statement.Query(username)
	defer rows.Close()
	if err != nil {
		log.Printf("Failed to query for username: %v. Err: %v", username, err)
		return nil, err
	}

	if rows.Next() {
		entry, err := userEntryFromRow(rows)
		if err != nil {
			panic(err.Error())
		}
		return entry, nil
	}

	return nil, errors.New(fmt.Sprintf("Failed to find any user entry with name %v", username))
}

func userEntryFromRow(rows *sql.Rows) (*UserEntry, error) {
	var entry UserEntry
	err := rows.Scan(
		&entry.Name,
		&entry.Salt,
		&entry.Hash,
	)

	if err != nil {
		log.Printf("Failed to scan row while fetching user entry. Err: %v", err)
		return nil, err
	}

	return &entry, nil
}

func generateSalt() string {
	salt := make([]byte, SALT_LEN)

	for i := 0; i < SALT_LEN; i++ {
		salt[i] = SALT_CHARS[rand.Intn(len(SALT_CHARS))]
	}
	
	saltStr := string(salt)
	return saltStr
}

func createUserEntry(username string, password string) (*UserEntry, error) {
	var entry UserEntry
	var err error

	entry.Name = username
	entry.Salt = generateSalt()

	entry.Hash, err = getHash(password, entry.Salt)
	if err != nil {
		log.Fatal("Failed to hash password. Dying.")
		return nil, err
	}

	return &entry, nil
}

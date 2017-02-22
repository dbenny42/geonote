package userdb

import (
	"log"
	"testing"
	"io/ioutil"

	"github.com/go-yaml/yaml"	
)

func TestUserdb(t *testing.T) {
	credentials, err := parseDbCredentials("testingCredentials.yaml")
	if err != nil {
		t.Fatal("Failed to parse db credentials. Err: ", err)
	}

	db, err := NewMysqlUserdb(credentials)
	if err != nil {
		t.Fatal("Failed to get a userdb handle")
	}

	username := "myusername"
	password := "password"

	// Run all subtests, now that common setup has occurred.
	t.Run("RegisterAndDelete", func(t *testing.T) {
		err = db.RegisterUser(username, password)
		if err != nil {
			t.Fatal("Failed to register new user.")
		}

		err = db.DeleteUser(username)
		if err != nil {
			t.Fatal("Failed to delete user.")
		}
	})

	t.Run("CheckCredentialsBadPassword", func(t *testing.T) {
		err = db.RegisterUser(username, password)
		if err != nil {
			t.Fatal("Failed to register new user.")
		}
		defer db.DeleteUser(username)

		validLogin, err := db.CheckCredentials(username, "badpassword")
		if err != nil {
			t.Fatal("Error while checking bad credentials.")
		}

		if validLogin {
			t.Fatal("Userdb accepted a bad password as a valid login.")
		}
	})

	t.Run("CheckCredentialsBadUsername", func(t *testing.T) {
		err = db.RegisterUser(username, password)
		if err != nil {
			t.Fatal("Failed to register new user.")
		}
		defer db.DeleteUser(username)

		validLogin, err := db.CheckCredentials("badusername", password)
		if err != nil {
			t.Fatal("Error while checking bad credentials.")
		}

		if validLogin {
			t.Fatal("Userdb accepted a bad username as a valid login.")
		}
	})

	t.Run("CheckCredentialsSucceeds", func(t *testing.T) {
		err = db.RegisterUser(username, password)
		if err != nil {
			t.Fatal("Failed to register new user.")
		}
		defer db.DeleteUser(username)

		validLogin, err := db.CheckCredentials(username, password)
		if err != nil {
			t.Fatal("Error while checking bad credentials.")
		}

		if !validLogin {
			t.Fatal("Userdb did not accept a correct login.")
		}
	})
}

func parseDbCredentials(filename string) (*DbCredentials, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Print("Failed to open db credentials file. err:", err)
		return nil, err
	}

	var credentials DbCredentials
	if err = yaml.Unmarshal(data, &credentials); err != nil {
		log.Print("Failed to unmarshal db credentials. Err:", err)
		return nil, err
	}
	
	return &credentials, nil
}

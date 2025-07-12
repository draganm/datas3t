package datas3t

import (
	"github.com/draganm/datas3t/crypto"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Datas3tServer struct {
	db        *pgxpool.Pool
	encryptor *crypto.CredentialEncryptor
}

func NewServer(db *pgxpool.Pool, encryptionKey string) (*Datas3tServer, error) {
	encryptor, err := crypto.NewCredentialEncryptor(encryptionKey)
	if err != nil {
		return nil, err
	}

	return &Datas3tServer{
		db:        db,
		encryptor: encryptor,
	}, nil
}

func (s *Datas3tServer) GetEncryptor() *crypto.CredentialEncryptor {
	return s.encryptor
}

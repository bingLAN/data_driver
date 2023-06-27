package common

import uuid "github.com/satori/go.uuid"

func GetUUID() string {
    id := uuid.NewV4()
    return id.String()
}

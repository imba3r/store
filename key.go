package thunder

import (
	"strings"
	"log"
	"fmt"
)

func IsCollectionKey(path string) bool {
	return len(strings.Split(path, "/")) % 2 == 1;
}

func IsDocumentKey(path string) bool {
	return !IsCollectionKey(path);
}

func CollectionKey(documentKey string) string {
	split := strings.Split(documentKey, "/")
	if len(split) < 2 || len(split) % 2 == 1 {
		log.Fatal("not a document key: ", documentKey)
	}
	return strings.TrimSuffix(documentKey, fmt.Sprintf("/%s", split[len(split)-1]))
}
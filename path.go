package thunder

import "strings"

func IsCollectionPath(path string) bool {
	return len(strings.Split(path, "/")) % 2 == 1;
}

func IsDocumentPath(path string) bool {
	return !IsCollectionPath(path);
}
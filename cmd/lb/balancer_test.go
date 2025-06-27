package main

import (
	"testing"
)

func TestChooseServerByPath(t *testing.T) {
	servers := []string{"server1", "server2", "server3"}
	path1 := "/some/path"
	server1, err1 := chooseServerByPath(path1, servers)
	if err1 != nil {
		t.Fatalf("Expected no error, got %v", err1)
	}
	server2, err2 := chooseServerByPath(path1, servers)
	if err2 != nil {
		t.Fatalf("Expected no error, got %v", err2)
	}

	if server1 != server2 {
		t.Errorf("Expected the same server for the same path, but got %s and %s", server1, server2)
	}
	path2 := "/another/path/entirely"
	server3, err3 := chooseServerByPath(path2, servers)
	if err3 != nil {
		t.Fatalf("Expected no error, got %v", err3)
	}
	if server3 == "" {
		t.Error("Expected a server to be returned, but got an empty string")
	}
	_, err4 := chooseServerByPath(path1, []string{})
	if err4 == nil {
		t.Error("Expected an error when server list is empty, but got nil")
	}
	singleServerList := []string{"lonely_server"}
	server4, err5 := chooseServerByPath(path1, singleServerList)
	if err5 != nil {
		t.Fatalf("Expected no error, got %v", err5)
	}
	if server4 != "lonely_server" {
		t.Errorf("Expected 'lonely_server', but got %s", server4)
	}
}

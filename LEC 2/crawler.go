package main

import (
    "fmt"
    "sync"
)

type Cache struct {
    visited   map[string]bool
    mux sync.Mutex
}

type Fetcher interface {
    // Fetch returns the body of URL and
    // a slice of URLs found on that page.
    Fetch(url string) (body string, urls []string, err error)
}

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Serial(url string, depth int, fetcher Fetcher, visited map[string]bool) {
    if depth <= 0 {
        return
    }

    if visited[url] {
        return
    }

    visited[url] = true
    
    body, urls, err := fetcher.Fetch(url)
    
    if err != nil {
        fmt.Println(err)
        return
    }
    
    fmt.Printf("found URL: %s ; title: %q\n", url, body)
    
    for _, u := range urls {
        Serial(u, depth - 1, fetcher, visited)   
    }
    
    return  
}


func ConcurrentMutex(url string, depth int, fetcher Fetcher, cache *Cache) {
    if depth <= 0 {
        return
    }
    
    cache.mux.Lock()
    if cache.visited[url] {
        cache.mux.Unlock()
        return
    }

    fmt.Println(url, cache.visited[url])
    
    cache.visited[url] = true
    cache.mux.Unlock()
    
    body, urls, err := fetcher.Fetch(url)
    
    if err != nil {
        fmt.Println(err)
        return
    }
    
    fmt.Printf("found URL: %s ; title: %q\n", url, body)
    
    var wg sync.WaitGroup
    
    for _, u := range urls {
        wg.Add(1)
        
        go func(u string) {
            defer wg.Done()
            ConcurrentMutex(u, depth - 1, fetcher, cache)
        }(u)    
    }
    
    wg.Wait()
    return  
}

func main() {
    fmt.Printf("=== Serial ===\n")
    Serial("https://golang.org/", 4, fetcher, make(map[string]bool))

    fmt.Printf("=== ConcurrentMutex ===\n")
    ConcurrentMutex("https://golang.org/", 4, fetcher, &Cache{visited: make(map[string]bool)})
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
    body string
    urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
    if res, ok := f[url]; ok {
        return res.body, res.urls, nil
    }
    return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
    "https://golang.org/": &fakeResult{
        "The Go Programming Language",
        []string{
            "https://golang.org/pkg/",
            "https://golang.org/cmd/",
        },
    },
    "https://golang.org/cmd/": &fakeResult{
        "Packages",
        []string{
            "https://golang.org/pkg/fmt/",
            "https://golang.org/pkg/os/",
            "https://golang.org/",
        },
    },
    "https://golang.org/pkg/": &fakeResult{
        "Packages",
        []string{
            "https://golang.org/",
            "https://golang.org/cmd/",
            "https://golang.org/pkg/fmt/",
            "https://golang.org/pkg/os/",
        },
    },
    "https://golang.org/pkg/fmt/": &fakeResult{
        "Package fmt",
        []string{
            "https://golang.org/",
            "https://golang.org/pkg/",
        },
    },
    "https://golang.org/pkg/os/": &fakeResult{
        "Package os",
        []string{
            "https://golang.org/",
            "https://golang.org/pkg/",
        },
    },
}
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

func ConcurrentChannel(url string, fetcher Fetcher) {
    ch := make(chan []string)
    go func() {
        ch <- []string{url}
    }()
    master(ch, fetcher)
}

func master(ch chan []string, fetcher Fetcher) {
    n := 1
    visited := make(map [string]bool)
    for urls := range ch {
        n -= 1
        for _, u := range urls {
            if !visited[u] {
                visited[u] = true
                n += 1
                go worker(u, ch, fetcher)

            }
        }

        if n == 0 {
            break
        }
    }
}

func worker(url string, ch chan []string, fetcher Fetcher) {
    body, urls, err := fetcher.Fetch(url)
    
    if err != nil {
        fmt.Println(err)
        ch <- []string{}
        return
    }

    fmt.Printf("found URL: %s ; title: %q\n", url, body)
    ch <- urls
}

func main() {
    fmt.Printf("=== Serial ===\n")
    Serial("https://golang.org/", 4, fetcher, make(map[string]bool))

    fmt.Printf("=== ConcurrentMutex ===\n")
    ConcurrentMutex("https://golang.org/", 4, fetcher, &Cache{visited: make(map[string]bool)})

    fmt.Printf("=== ConcurrentChannel ===\n")
    ConcurrentChannel("https://golang.org/", fetcher)
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
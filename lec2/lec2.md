# 第二节

## 课程部分: RPC & Threads

### 分布式系统中IO并发和多核并发的处理方式: 线程管理

其实lec2就是主要根据那个教程中的爬虫案例来讲的，所以在做之前一定记得先把go tour学完，不然就会像我一样听了半天，然后“唉？这是案例吗？我咋不知道？？？”

#### 线程与进程的区别:

进程有自己独立的资源，比如内存空间，数据和线程。而线程则是多个线程共享一个进程中的资源，并且进行协同处理，如果一个线程出错，可能导致整个进程停止工作。

进程是资源调度的基本单位，而线程是CPU调度的基本单位。进程可以在多核CPU上并行，而线程可以在同一个进程内进行并发。对于单一进程而言，多线程操作可以有效减少I/O时间，提高CPU的利用效率。

对于需要高度隔离的情况下可以创建进程，而任务需要频繁的上下文切换和数据共享时，要使用多线程。

#### 线程竞争问题:

在Go语言中，使用锁来解决线程竞争的问题。但是要注意死锁的情况，许多情况下，分布式系统出现程序停止运行的问题，是由于死锁的存在导致的。

#### web爬虫题目

```Go

package main

import (
	"fmt"
	"sync"
)

type Fetcher interface {
	// Fetch 返回 URL 所指向页面的 body 内容，
	// 并将该页面上找到的所有 URL 放到一个切片中。
	Fetch(url string) (body string, urls []string, err error)
}

type UrlRecord struct {
	v  map[string]int
	mu sync.Mutex
	wg sync.WaitGroup
}

var m = UrlRecord{v: make(map[string]int)}

// Crawl 用 fetcher 从某个 URL 开始递归的爬取页面，直到达到最大深度。
func Crawl(url string, depth int, fetcher Fetcher) {
	defer m.wg.Done()
	if depth <= 0 {
		return
	}
	m.mu.Lock()
	m.v[url]++
	m.mu.Unlock()

	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("found %s %q \n", urls, body)

	for _, u := range urls {
		m.mu.Lock()
		if _, ok := m.v[u]; !ok {
			m.wg.Add(1)
			go Crawl(u, depth-1, fetcher)
		}
		m.mu.Unlock()
	}
	return
}

func main() {
	m.wg.Add(1)
	Crawl("https://golang.org/", 4, fetcher)
	m.wg.Wait()
}

// fakeFetcher 是待填充结果的 Fetcher。
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

// fetcher 是填充后的 fakeFetcher。
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
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

```

这个爬虫程序利用了互斥锁，创建全局的UrlRecord实例，包含线程安全的map和同步工具

通过逐层爬取，获取网页内容和链接。使用sync.Mutex保护共享的map访问，使用sync.WaitGroup等待所有goroutine完成。


// Name: Nisa
// Surname: Yakut
// StudentID: 231ADB107
//
// gosort - Concurrent Chunk Sorting
// Modes:
//   -r N            generate N random integers (N >= 10)
//   -i input.txt    read integers from file (one per line, ignore empty lines)
//   -d incoming     sort all .txt files in directory, write to sibling output directory
package main

import (
	"bufio"
	"container/heap"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// Used only for -d output directory naming:
	firstName = "nisa"
	surname   = "yakut"
	studentID = "YOUR_STUDENT_ID_HERE"

	// Random range (documented): integers in [-1000, 1000]
	randMin = -1000
	randMax = 1000
)

func main() {
	rN := flag.Int("r", -1, "generate N random integers (N >= 10)")
	inFile := flag.String("i", "", "input file with one integer per line")
	inDir := flag.String("d", "", "directory containing .txt files to sort")
	flag.Parse()

	// Exactly one mode must be used
	modesUsed := 0
	if *rN != -1 {
		modesUsed++
	}
	if *inFile != "" {
		modesUsed++
	}
	if *inDir != "" {
		modesUsed++
	}
	if modesUsed != 1 {
		exitErr("Use exactly one mode: -r, -i, or -d")
	}

	switch {
	case *rN != -1:
		if *rN < 10 {
			exitErr("N must be >= 10")
		}
		nums := generateRandom(*rN)
		runAndPrint(nums)

	case *inFile != "":
		nums, err := readIntsFromFile(*inFile)
		if err != nil {
			exitErr(err.Error())
		}
		if len(nums) < 10 {
			exitErr("Input must contain at least 10 valid integers")
		}
		runAndPrint(nums)

	case *inDir != "":
		if err := processDirectory(*inDir); err != nil {
			exitErr(err.Error())
		}
	}
}

// ---------- Core Pipeline ----------

func runAndPrint(nums []int) {
	fmt.Println("Original numbers:")
	fmt.Println(nums)

	chunks := makeChunks(nums)
	fmt.Println("\nChunks before sorting:")
	printChunks(chunks)

	sortChunksConcurrently(chunks)
	fmt.Println("\nChunks after sorting:")
	printChunks(chunks)

	merged := mergeChunks(chunks)
	fmt.Println("\nFinal merged sorted result:")
	fmt.Println(merged)
}

// ---------- Chunking ----------

func makeChunks(nums []int) [][]int {
	n := len(nums)
	k := int(math.Ceil(math.Sqrt(float64(n))))
	if k < 4 {
		k = 4
	}

	base := n / k
	rem := n % k

	chunks := make([][]int, 0, k)
	idx := 0
	for i := 0; i < k; i++ {
		size := base
		if i < rem {
			size++
		}
		if size == 0 {
			continue
		}
		chunk := make([]int, size)
		copy(chunk, nums[idx:idx+size])
		chunks = append(chunks, chunk)
		idx += size
	}
	return chunks
}

// ---------- Concurrent Sorting ----------

func sortChunksConcurrently(chunks [][]int) {
	var wg sync.WaitGroup
	wg.Add(len(chunks))
	for i := range chunks {
		go func(i int) {
			defer wg.Done()
			sort.Ints(chunks[i])
		}(i)
	}
	wg.Wait()
}

// ---------- Merge (k-way merge, NO re-sort) ----------

type item struct {
	value int
	ci    int // chunk index
	pi    int // position in chunk
}

type minHeap []item

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].value < h[j].value }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(item)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func mergeChunks(chunks [][]int) []int {
	h := &minHeap{}
	heap.Init(h)

	total := 0
	for ci, ch := range chunks {
		if len(ch) > 0 {
			heap.Push(h, item{value: ch[0], ci: ci, pi: 0})
			total += len(ch)
		}
	}

	res := make([]int, 0, total)
	for h.Len() > 0 {
		it := heap.Pop(h).(item)
		res = append(res, it.value)
		if it.pi+1 < len(chunks[it.ci]) {
			heap.Push(h, item{
				value: chunks[it.ci][it.pi+1],
				ci:    it.ci,
				pi:    it.pi + 1,
			})
		}
	}
	return res
}

// ---------- Modes Helpers ----------

func generateRandom(n int) []int {
	rand.Seed(time.Now().UnixNano())
	nums := make([]int, n)
	for i := 0; i < n; i++ {
		nums[i] = rand.Intn(randMax-randMin+1) + randMin
	}
	return nums
}

func readIntsFromFile(path string) ([]int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var nums []int
	sc := bufio.NewScanner(f)
	line := 0
	for sc.Scan() {
		line++
		txt := strings.TrimSpace(sc.Text())
		if txt == "" {
			continue
		}
		v, err := strconv.Atoi(txt)
		if err != nil {
			return nil, fmt.Errorf("invalid integer at line %d", line)
		}
		nums = append(nums, v)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return nums, nil
}

func processDirectory(dir string) error {
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return errors.New("directory not found")
	}

	parent := filepath.Dir(dir)
	base := filepath.Base(dir)
	outDir := filepath.Join(parent,
		fmt.Sprintf("%s_sorted_%s_%s_%s", base, firstName, surname, studentID))

	if err := os.MkdirAll(outDir, 0755); err != nil {
		return err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".txt" {
			continue
		}
		inPath := filepath.Join(dir, e.Name())
		nums, err := readIntsFromFile(inPath)
		if err != nil {
			return err
		}
		if len(nums) < 10 {
			return fmt.Errorf("file %s has fewer than 10 numbers", e.Name())
		}
		chunks := makeChunks(nums)
		sortChunksConcurrently(chunks)
		sorted := mergeChunks(chunks)

		outPath := filepath.Join(outDir, e.Name())
		if err := writeInts(outPath, sorted); err != nil {
			return err
		}
	}
	return nil
}

func writeInts(path string, nums []int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, v := range nums {
		fmt.Fprintln(w, v)
	}
	return w.Flush()
}

// ---------- Utils ----------

func printChunks(chunks [][]int) {
	for i, ch := range chunks {
		fmt.Printf("Chunk %d: %v\n", i, ch)
	}
}

func exitErr(msg string) {
	fmt.Fprintln(os.Stderr, "Error:", msg)
	os.Exit(1)
}

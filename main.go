package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type input struct {
	buf       []byte
	lineStart int
}

type worker struct {
	dateCount map[uint64]int
	nameCount map[string]*int
	buf       []byte
	le        Extraction
	name      Name

	input  chan input
	output chan bool

	parent *pool
}

func (w *worker) job() {
	for {
		select {
		case <-w.input:
			// This means the job was closed
			close(w.output)
			w.parent.wg.Done()
			return
		case w.parent.offer <- w:
		}

		// We are here now. This means the pool is going to give this worker a job
		in := <-w.input

		buf := in.buf
		i := in.lineStart
		for len(buf) > 0 {
			pos := bytes.IndexByte(buf, '\n')
			var line []byte
			if pos >= 0 {
				line = buf[:pos]
				buf = buf[pos+1:]
			} else {
				line = buf
				buf = buf[len(buf):]
			}

			if ok, err := w.le.Extract(line); !ok {
				if err != nil {
					log.Printf("failed to extract line \033[1m%s\033[0m: %s", string(line), err)
				} else {
					log.Printf("failed to extract line \033[1m%s\033[0m", string(line))
				}
				continue
			}
			switch i {
			case 0:
				fmt.Printf("Name: %s at index: %v\n", string(w.le.Name), 0)
			case 432:
				fmt.Printf("Name: %s at index: %v\n", string(w.le.Name), 432)
			case 43243:
				fmt.Printf("Name: %s at index: %v\n", string(w.le.Name), 43243)
			}
			i++

			// extract dates
			w.dateCount[w.le.Time/1000000000000]++

			name := w.le.Name
			if ok, _ := w.name.Extract(name); ok {
				name = w.name.First
			}
			if pos := bytes.IndexByte(name, ','); pos >= 0 {
				name = name[:pos]
			}
			if v, ok := w.nameCount[string(name)]; ok {
				*v++
			} else {
				vv := 1
				w.nameCount[string(name)] = &vv
			}

		}
	}
}

type pool struct {
	wg      *sync.WaitGroup
	workers []*worker

	offer chan *worker

	current int
}

const bufSize = 1024 * 1024

func newPool(n int) *pool {
	p := &pool{
		wg:    &sync.WaitGroup{},
		offer: make(chan *worker, n),
	}
	for i := 0; i < n; i++ {
		p.wg.Add(1)
		w := &worker{
			dateCount: make(map[uint64]int, 1000000),
			nameCount: make(map[string]*int, 1000000),
			buf:       make([]byte, bufSize),
			input:     make(chan input, 36),
			output:    make(chan bool),
			parent:    p,
		}
		p.workers = append(p.workers, w)

		go w.job()
	}

	return p
}

func (p *pool) wait() {
	for _, w := range p.workers {
		close(w.input)
	}
	p.wg.Wait()
}

func cutUnfinishedLine(buf []byte) (head []byte, tail []byte) {
	if len(buf) == 0 {
		return nil, nil
	}
	if len(buf) < 256 {
		// Buffers are supposed to be large and tiny buffer can only mean we have reached an end
		return buf, nil
	}

	jump := 256
	for {
		if jump > len(buf) {
			jump = len(buf)
		}
		rest := buf[len(buf)-jump:]
		if pos := bytes.IndexByte(rest, '\n'); pos >= 0 {
			return buf[:len(buf)-jump+pos], buf[len(buf)-jump+pos+1:]
		} else {
			if jump == len(buf) {
				return buf, nil
			}
			jump *= 2
		}
	}
}

var newLineSep = []byte("\n")

func main() {
	start := time.Now()

	file, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	p := newPool(16)
	var rest []byte
	var lineCount int
	for {
		w := <-p.offer
		buf := w.buf
		copy(buf, rest)
		read, err := file.Read(buf[len(rest):])
		if err == io.EOF {
			w.input <- input{
				buf:       rest,
				lineStart: lineCount,
			}
			lineCount += bytes.Count(rest, newLineSep)
			break
		}
		if err != nil {
			fmt.Printf("error reading input file: %s", err)
			os.Exit(1)
		}

		buf = buf[:len(rest)+read]
		buf, rest = cutUnfinishedLine(buf)

		w.input <- input{
			buf:       buf,
			lineStart: lineCount,
		}
		lineCount += bytes.Count(buf, newLineSep) + 1
	}
	p.wait()

	fmt.Printf("Name time: %v\n", time.Since(start))

	// report c1: total number of lines
	fmt.Printf("Total file line count: %v\n", lineCount)
	fmt.Printf("Line count time: : %v\n", time.Since(start))

	dateMap := make(map[uint64]int)
	for _, w := range p.workers {
		for k, v := range w.dateCount {
			dateMap[k] += v
		}
	}

	for k, v := range dateMap {
		fmt.Printf("Donations per month and year: %v and donation count: %v\n", k, v)
	}
	fmt.Printf("Donations time: : %v\n", time.Since(start))

	names := make(map[string]int)
	for _, w := range p.workers {
		for k, v := range w.nameCount {
			names[k] += *v
		}
	}

	commonName := ""
	commonCount := 0
	for name, count := range names {
		if count > commonCount {
			commonName = name
			commonCount = count
		}
	}

	fmt.Printf("The most common first name is: %s and it occurs: %v times.\n", commonName, commonCount)
	_, _ = fmt.Fprintf(os.Stderr, "revision: %v, runtime: %v\n", filepath.Base(os.Args[0]), time.Since(start))
}

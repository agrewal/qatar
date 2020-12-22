package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/agrewal/qatar"
	"github.com/segmentio/ksuid"
)

func main() {
	countCmd := flag.NewFlagSet("count", flag.ExitOnError)
	cntQDir := countCmd.String("q", "", "Directory path")

	enqCmd := flag.NewFlagSet("enq", flag.ExitOnError)
	enqQDir := enqCmd.String("q", "", "Directory path")

	delCmd := flag.NewFlagSet("del", flag.ExitOnError)
	delQDir := delCmd.String("q", "", "Directory path")

	lsCmd := flag.NewFlagSet("ls", flag.ExitOnError)
	lsQDir := lsCmd.String("q", "", "Directory path")
	lsN := lsCmd.Int("n", 10, "Number of queue items to display")
	lsS := lsCmd.Bool("s", false, "Treat value as string")

	createCmd := flag.NewFlagSet("create", flag.ExitOnError)
	createQDir := createCmd.String("q", "", "Directory path")

	if len(os.Args) < 2 {
		fmt.Println("Expected a subcommand")
	}

	switch os.Args[1] {
	case "create":
		createCmd.Parse(os.Args[2:])
		if len(*createQDir) == 0 {
			fmt.Println("Must specify queue directory")
			os.Exit(1)
		}
		q, err := qatar.CreateQ(*createQDir)
		if err != nil {
			panic(err)
		}
		defer q.Close()

	case "count":
		countCmd.Parse(os.Args[2:])
		if len(*cntQDir) == 0 {
			fmt.Println("Must specify queue directory")
			os.Exit(1)
		}
		q, err := qatar.OpenQ(*cntQDir)
		if err != nil {
			panic(err)
		}
		defer q.Close()
		count, err := q.Count()
		if err != nil {
			panic(err)
		}
		fmt.Println(count)

	case "enq":
		enqCmd.Parse(os.Args[2:])
		if len(*enqQDir) == 0 {
			fmt.Println("Must specify queue directory")
			os.Exit(1)
		}
		q, err := qatar.OpenQ(*enqQDir)
		if err != nil {
			panic(err)
		}
		defer q.Close()
		if len(enqCmd.Args()) != 1 {
			fmt.Println("Should have exactly one string to enqueue")
			os.Exit(1)
		}
		id, err := q.Enqueue([]byte(enqCmd.Args()[0]))
		if err != nil {
			panic(err)
		}
		fmt.Println(id.String())

	case "del":
		delCmd.Parse(os.Args[2:])
		if len(*delQDir) == 0 {
			fmt.Println("Must specify queue directory")
			os.Exit(1)
		}
		q, err := qatar.OpenQ(*delQDir)
		if err != nil {
			panic(err)
		}
		defer q.Close()
		if len(delCmd.Args()) < 1 {
			fmt.Println("Should have atleast 1 id to delete")
			os.Exit(1)
		}
		for _, id := range delCmd.Args() {
			kid, err := ksuid.Parse(id)
			if err != nil {
				panic(err)
			}
			err = q.Delete(kid)
			if err != nil {
				panic(err)
			}
		}

	case "ls":
		lsCmd.Parse(os.Args[2:])
		if len(*lsQDir) == 0 {
			fmt.Println("Must specify queue directory")
			os.Exit(1)
		}
		q, err := qatar.OpenQ(*lsQDir)
		if err != nil {
			panic(err)
		}
		defer q.Close()

		items, err := q.PeekMulti(*lsN)
		if err != nil {
			panic(err)
		}
		for _, item := range items {
			if *lsS {
				fmt.Printf("%s: %q\n", item.Id.String(), string(item.Data))
			} else {
				fmt.Println(item.Id.String())
			}
		}

	default:
		fmt.Printf("Unknown subcommand %s\n", os.Args[1])
		os.Exit(1)
	}
}

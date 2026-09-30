package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		if activeDashboard != nil {
			activeDashboard.Stop()
		}
		fmt.Print("\033[?25h\n\n[Download cancelled by user. Clean shutdown complete.]\n")
		os.Exit(0)
	}()

	if len(os.Args) >= 2 {
		arg := os.Args[1]
		if arg == "--demo" || arg == "-d" || arg == "--local-demo" {
			out := "demo-download.iso"
			if len(os.Args) >= 3 {
				out = os.Args[2]
			}
			RunControlledDemo(out)
			return
		}
		if arg == "--test-dashboard" || arg == "--test-center" || arg == "-t" {
			RenderTestCenter()
			return
		}
	}

	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "BitTorrent Client with Dual Transport (TCP + μTP / UDP)\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  %s <torrent file> <output file>            (Live Interactive Monitor)\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s --demo [output file]                    (Offline Local μTP Demonstration)\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s --test-dashboard                        (Protocol Test Center Matrix)\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s --headless <torrent file> <output file> (Headless Logging Mode)\n", os.Args[0])
		os.Exit(1)
	}

	headless := false
	torrent_path := os.Args[1]
	output_path := os.Args[2]

	if os.Args[1] == "--headless" || os.Args[1] == "--no-dashboard" {
		if len(os.Args) < 4 {
			fmt.Fprintf(os.Stderr, "usage: %s --headless <torrent file> <output file>\n", os.Args[0])
			os.Exit(1)
		}
		headless = true
		torrent_path = os.Args[2]
		output_path = os.Args[3]
	}

	tf, err := open(torrent_path)
	if err != nil {
		log.Fatalf("could not open torrent file: %v\n", err)
	}

	dash := InitDashboard(tf.name, tf.length, len(tf.piece_hashes), !headless)
	if !headless {
		dash.RenderStartup()
		dash.StartBackgroundRefresh()
	}

	data, err := tf.download()
	if err != nil {
		dash.Stop()
		log.Fatalf("download failed: %v\n", err)
	}
	dash.Stop()

	err = os.WriteFile(output_path, data, 0644)
	if err != nil {
		log.Fatalf("could not write output file: %v\n", err)
	}

	if !headless {
		dash.RenderCompletion(data, output_path)
	} else {
		fmt.Printf("downloaded %s to %s\n", tf.name, output_path)
	}
}

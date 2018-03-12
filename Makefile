PACKAGENAME=dat
COLLECTS=dat protobuf blake2b bittorrent-kademlia syndicate

all: setup

clean:
	find . -name compiled -type d | xargs rm -rf

setup:
	raco setup $(COLLECTS)

link:
	raco pkg install --link -n $(PACKAGENAME) $(CURDIR)/src

unlink:
	raco pkg remove $(PACKAGENAME)

test:
	raco test --package $(PACKAGENAME)

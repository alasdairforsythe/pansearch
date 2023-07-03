# pansearch

This is heavily modified version of what was originally an extremely efficient binary search data structure I'd made a few years back for checking strings against a dictionary, and for counting occurances of strings. It packs strings into arrays of uint64, allowing for much faster comparisons and very low memory overhead.

This modified version is the backend to [TokenMonster](https://github.com/alasdairforsythe/tokenmonster). It uses a combination of binary search, sorting, hashmaps, lookup tables and bloom filters.

I would write usage instructions but I suspect nobody will find it. In case you really want to use it, you can make an issue and ask for the usage instructions.
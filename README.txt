Nick Herzig
CSC 369 Lab 2

Question 1:
For question 1, I used URLs as the Key in the map, and eachh value was one instance of the Url.
The reduce phase then added up each count of the key to emit the URLs and their counts to the file.
Then, AccessLog2 used to sort the counts by ascending order. AccessLog2 takes the counts as Keys,
so all URLs of the same count are grouped together. The counts are sorted in the shuffle phase 

Question 2:
Use the response codes as the Key, which will be automatically sorted during the shuffle period.
And then the counts are entered in as the values, which are summed up during the reduce period.

Question 3:
For question 3, I hardcoded "64.242.88.10" as the ip address to match with. Then, if a line was
associated with that ip address, I would use the ip as the key and the bytes as the value.
The reduce summed all the bytes together and ouputted that line.

Question 4:
For question 4, I hardcoded "/twiki/bin/view/Main/WebHome" as the URL. Each line with this URL was written to
the map with Users as keys and intOne as the value. The counts were then summed up in reduce.
AccessLog2 is used by passing the counts in the output as input to the map, and then reduce outputs all users for each count.

Question 5:
Question 5 took some careful splitting of the time stamp. I created a key that consisted of Year/Month, with Month being switched to its
numerical format. This allowed the key to be sorted automatically

Question 6:
I passed in the date as a key, and then number of bytes for each access line.
AccessLog 2 takes that output as input, where count is the byte is the key, and that key is then sorted through the shuffle period.
# cs425-mp2

This is the repository for CS425 MP2. In this MP we write a program that enable multiple virtual machines to work as a distributed group membership.



## Usage
You should run ```server.py``` on the provided virtual machines.
### Start Server
> $ python3 server.py

### Commands
Once the server is started, it will show the commands which you can use and the program will continously run util you kill the process.

You can input the following commands(one at a time):

- `join`: let the process join the group
- `list_mem`: list the membership list
- `list_self`: list self process's id. The format is ```{IP address}#timestamp```
- `leave`:  command to voluntarily leave the group (different from a failure, which will be Ctrl-C or kill)
- `grep`: get into MP1 distributed grep program

## Output
If the program runs successfully, your terminal will show the following based on your input:
```
Please enter input: 3
Selected join the group
start joining
```

```
Please enter input: 1
Selected list_mem
{'fa22-cs425-5701.cs.illinois.edu': ('1664158262', 'RUNNING'),
'fa22-cs425-5710.cs.illinois.edu': ['1664158262', 'RUNNING'],
'fa22-cs425-5709.cs.illinois.edu': ['1664158257', 'LEAVE'],
'fa22-cs425-5708.cs.illinois.edu': ('1664158262', 'RUNNING'),
'fa22-cs425-5707.cs.illinois.edu': ('1664158262', 'RUNNING'),
'fa22-cs425-5706.cs.illinois.edu': ('1664158261', 'RUNNING')}
```

```
Please enter input: 2
Selected list_self
172.22.94.188#1664157284
```

```
Please enter input: 4
Selected voluntarily leave
{'fa22-cs425-5701.cs.illinois.edu': ('1664158262', 'LEAVE'),
'fa22-cs425-5710.cs.illinois.edu': ['1664158262', 'RUNNING'],
'fa22-cs425-5709.cs.illinois.edu': ['1664158257', 'LEAVE'],
'fa22-cs425-5708.cs.illinois.edu': ('1664158262', 'RUNNING'),
'fa22-cs425-5707.cs.illinois.edu': ('1664158262', 'RUNNING'),
'fa22-cs425-5706.cs.illinois.edu': ('1664158261', 'RUNNING')}
```
## Authors
- Zerun Zhao(zerunz2)
- Han Xu(hanxu8)

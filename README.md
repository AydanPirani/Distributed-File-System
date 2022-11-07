This is the repository for CS425 MP2. In this MP we write a program that enable multiple virtual machines to work as a distributed group membership.

## Usage
You should run ```server.py``` on the provided virtual machines.
### Start Server
> $ python3 server.py

### Commands
Once the server is started, it will show the commands which you can use and the program will continously run util you kill the process.

You can input the following numbers for each command one at a time and enter the input that it asks for.

    0. exit: exit the file system
    1. put: add a file to the file system
    2. get: get a file from the file system
    3. delete: delete a file from the file system
    4. ls: print all files in the filesystem
    5. store: list all files being stored in the machine
    6. get-versions: get the last N versions of the file in the machine
    7. list_mem: list the membership list"
    8. list_self: list self's id"
    9. leave: command to voluntarily leave the group (different from a failure, which will be Ctrl-C or kill)"

For example, to run put, the terminal would look like the following:

    Please enter command: 1
    Selected put
    Enter local filename: hi
    local file does not exist! please try again
    Please enter command: 1 
    Selected put
    Enter local filename: v3
    Enter SDFS filename: v
    Finished Operation! Time Taken: 0:00:01.405673

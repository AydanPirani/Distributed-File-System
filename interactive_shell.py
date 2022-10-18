# print the starting message
print("Welcome to the interactive shell for CS425 MP2. You may press 1/2/3/4 for below functionalities.\n"
      "1. list_mem: list the membership list\n"
      "2. list_self: list self’s id\n"
      "3. join: command to join the group\n"
      "4. leave: command to voluntarily leave the group (different from a failure, which will be Ctrl-C or kill)"
      )

# interactive shell
while 1:
    input_str = input("Awaiting user input... ")
    if input_str == 'exit':
        break
    if input_str == "1":
        print("Selected list_mem")
        continue
    elif input_str == "2":
        print("Selected list_self")
        continue
    elif input_str == "3":
        print("Selected join the group")
        continue
    elif input_str == "4":
        print("Selected voluntarily leave")
        continue
    else:
        print("Invalid input. Please try again")


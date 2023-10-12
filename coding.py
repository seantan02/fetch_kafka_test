#Define the variables
array = [0,1,2,4,50, 75]
lower = 0
upper = 99
#We keep track of not_exist and have an i as number from lower to upper
not_exist = []
i = 0
final_string = ""
#We loop throught i until upper bound
while i < upper+1:
    #Give lat_not_exist a value if there exist
    last_not_exist = not_exist[-1] if len(not_exist) > 0 else None
    #If i is not in array
    if i not in array:
        #We append it as string
        not_exist.append(str(i))
        #Now we loop through and find the next biggest integer that is missing by while
        while i+1 not in array and i< upper:
            i = i+1
        #If i > last appended not_exist, that means there's a range ->
        if i > int(not_exist[-1]):
            not_exist[-1] += "->"+str(i)
    i += 1
#Now we loop through the not_exist and make it a string to print it out
for i in range(len(not_exist)):
    string = not_exist[i]
    final_string += string+", " if i != len(not_exist)-1 else string
#Print our final string
print(final_string)
#This is a O(N) complexity which is way faster than what I shown

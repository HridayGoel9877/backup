#print prime numbers
#check number is prime
#two sum
#remove duplicate
#palindrome
#reverse a string
#reverse a list
#FACTORAIL OF A NUMBER
#SECOND LARGEST IN A LIST
#COUNT VOWELS IN A STRING
#fibonacci series
#find duplicate
#find missing numbers
#Convert Integer to the Sum of Two No-Zero Integers
#Count Frequency of Each Element


#OPPS CONCEPT
#LIST COMPRHENSION
#TYPES OF FUNCTIONS LIKE USER DEFINED LAMBDA BUILT IN
#WHAT IS LAMBDA FUNCTIONS
#SQL JOINS QUESTIONS



#PRINT PRIME NUMBERS
# n=int(input("enter the num:"))
# for i in range(2,n):
#     is_prime=True
#     for j in range(2,i):
#         if i%j==0:
#             is_prime=False
#     if is_prime:
#         print(i)



#CHECK NUMBER IS PRIME
# n=int(input("enter num:"))
# if n<=2:
#     print("not prime")
# else:
#     for i in range(2,n):
#         is_prime=True
#         if n%i==0:
#             is_prime=False
#             print("false")
#             break
#     if is_prime:
#         print("true")


#TWO SUM
# a=[1,2,3,4,5]
# arr=[]
# target=int(input("enter to check:"))
# for i in range(len(a)):
#     for j in range(i+1,len(a)):
#         if a[i]+a[j]==target:
#             arr.append((a[i],a[j]))
# print(arr)

#ANOTHER METHOD AND THE OPTIMISED ONE
# ans=[]
# arr=[1,2,3,4,5]
# for i in range(len(arr)):
#     if(5-arr[i] in arr and (5-arr[i],arr[i]) not in ans):
#         ans.append((arr[i],5-arr[i]))
# print(ans)


#REMOVE DUPLICATE
# a=[1,1,2,2,3,4,4,5]
# li=[]
# for i in range(len(a)):
#     if a[i] not in li:
#         li.append(a[i])
# print(li,"length of list:",[len(li)])

#ANOTHER METHOD
# a=[1,2,2,3,4,5,5,6]
# li=list(set(a))
# print(li)


#PALINDROME
# # stri="madam"
# li=12211
# stri=str(li)
# is_palin=True
# for i in range(len(stri)):
#     j=len(stri)-i-1
#     if stri[i]!=stri[j]:
#         is_palin=False
#         print("not palin")
#         break
# if is_palin:
#     print("True")

#REVERSE A STRING
# stri="hriday"
# reverse=""
# for i in range(len(stri)-1,-1,-1):
#     reverse=reverse+stri[i]
# print(reverse)

#ANOTHER METHOD
# stri="hriday"
# reverse=""
# for i in range(len(stri)):
#     reverse=stri[i]+reverse
# print(reverse)


#REVERSE A LIST
# li=[1,2,3,4,5]
# reverse=[]
# for i in range(len(li)-1,-1,-1):
#     reverse.append(li[i])
# print(reverse)

#FACTORAIL OF A NUMBER
# n=int(input("enter the number:"))
# fact=1
# for i in range(1,n+1):
#     fact=fact*i
# print(fact)

#SECOND LARGEST IN A LIST
# arr=[1,2,4,5,4,5,7]
# arr.sort()
# arr2=list(set(arr))
# print(arr2[-2])


#COUNT VOWELS IN A STRING
# stri="hriday"
# vowels='a','e','i','o','u','A','E','I','O','U'
# count=0
# for i in range(len(stri)):
#     if stri[i] in vowels:
#         count+=1
# print(count)


#FIBONACCI SERIES
# a=0
# b=1
# print(a)
# print(b)
# for i in range(2,5):
#     c=a+b
#     print(c)
#     a=b
#     b=c

#ANOTHER METHOD AND MORE USUABLE ONE
# a=0
# b=1
# for i in range(6):
#     print(a)
#     c=a+b
#     a=b
#     b=c


#FIND DUPLICATE
# arr=[1,2,2,3,3,4,5,5,5]
# dup=[]
# seen=[]
# for i in range(len(arr)):
#     if arr[i] in seen and arr[i] not in dup:
#         dup.append(arr[i])
#     else:
#         seen.append(arr[i])
# print(dup)


#FIND THE MISSING NUMBER
# arr=[0,1,2,4,5,6]
# arr.sort()
# for i in range(len(arr)+1):
#     if (i not in arr):
#         print(i,end=" ") 


#Convert Integer to the Sum of Two No-Zero Integers
# n=int(input("n"))
# for i in range(1,n):
#     a=i
#     b=n-i
#     if "0" not in str(a) + str(b):
#         print(a,b)
#         break


#COUNT FREQUENCY OF EACH ELEMENT
# stri="hriday"
# freq={}
# for i in range(len(stri)):
#     if stri[i] in freq:
#         freq[stri[i]]+=1
#     else:
#         freq[stri[i]]=1
# print(freq)

# ANOTHER METHOD
# arr = [1, 2, 2, 3, 3, 3, 4]
# freq = {}

# for i in arr:
#     count = 0
#     for j in arr:
#         if i == j:
#             count += 1
#     freq[i] = count

# print(freq)   # {1:1, 2:2, 3:3, 4:1}



matrix = [[1,3,5,7],[10,13,3,20],[23,30,34,60]]

# for i in range(len(matrix)):
#     print(matrix[i])
#     for j in range(len(matrix[i])):
#         print(matrix[i][j])



# for i in matrix:
#     found=False
#     for j in i:
#         if j==13:
#             found=True
#             break
#     if found:
#         break
# if found:
#     print("true")
# else:
#     print("false")



# n=int(input("enter"))
# b=int(input("enter b:"))
# j=str(input("enter j:"))

# # for i in range(1,n+1):
# def calculator(num1,num2,operator):
#     if operator=="+":
#         print(num1+num2)
#     elif operator=="*":
#         print(num1*num2)
#     else:
#         print("enter valid")

# calculator(n,b,j)
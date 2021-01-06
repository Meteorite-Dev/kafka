Compare = lambda M1,M2 : True if M1==M2 else False
Match = lambda C1,list : C1 in list

lis = [1,2,3,4,5,6,7,8,9,0]
t = Match(1,lis)
print(t)
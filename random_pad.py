def code(cust):
    i=0
    j=0
    n=len(cust)
    d=defaultdict()
    t=""
    k=n
    entity=[]

    while (j<=n):
        if cust[j]!="_":
            j+=1

        elif cust[j]=="_" and j-i>1:
            d[cust[i:j]]+=1
            i==j
            j+=1

        elif cust[j]=="_" and j-i<=1:
            j+=1
            i==j

        if cust[k-1]!="_" and k!=0:
            k-=1
        elif cust[k-1]=="_" and k!=0:
            entity.append(cust[k-1:len(cust)][-1::-1])

    l=""
    for i in d.keys():
        if i not in ("BVI"):
            l+=i
            l+="_"



            

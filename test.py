# n is number of elements, i is set of interest, j is number of selections
# Probability of picking none of the i elements
def p(n,i,j):
   return scipy.misc.comb(n - i, j, 1) / float(scipy.misc.comb(n, j, 1))


def plotnone(n):
   for j in range(1,n+1):
       irange = range(1,n+1)
       probs = []
       for i in irange:
          probs.append(p(n,i,j))
       plt.plot(irange, probs, '.-')
   plt.show()


def plot(n):
   for j in range(1,n+1):
       irange = range(1,n+1)
       probs = []
       for i in irange:
          probs.append(1 - p(n,i,j))
       plt.plot(irange, probs, '.-')
   plt.show()
import scipy.misc
from matplotlib import pyplot as plt

# n is number of elements, i is set of interest, j is number of selections
# Probability of picking none of the i elements
def p(n,i,j):
   return scipy.misc.comb(n - i, j, 1) / float(scipy.misc.comb(n, j, 1))


def plotnone(n):
   for j in range(1,n+1):
       irange = range(1,n+1)
       probs = []
       for i in irange:
          prob = p(n,i,j)
          probs.append(prob)
          if j < 4: print prob
       plt.plot(irange, probs, '.-')
   plt.show()


def plot(n):
   for j in range(1,n+1):
       irange = range(1,n+1)
       probs = []
       for i in irange:
          
          prob = 1-p(n,i,j)
          if j < 4 : print prob
          probs.append(prob)
       plt.plot(irange, probs, '.-')
   plt.show()

plotnone(20)


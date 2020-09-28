
h = 1000
x = seq(-1000, 1000, 1)
y = exp(-(x/h)^2)
plot(x, y, type='l', xlab='Possible distances', ylab='Kernel', main='Distance kernel')

h = 2
x = seq(-12, 12, 0.3)
y = exp(-(x/h)^2)
plot(x, y, type='l', xlab='Possible time differences', ylab='Kernel', main='Time kernel')

h = 12
x = seq(-180, 180, 1)
y = exp(-(x/h)^2)
plot(x, y, type='l', xlab='Possible day differences', ylab='Kernel', main='Date kernel')

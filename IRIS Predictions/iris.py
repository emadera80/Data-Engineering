from sklearn.datasets import load_iris
from sklearn.neighbors import KNeighborsClassifier 
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score


iris = load_iris()

X = iris.data
y = iris.target  

X_train, X_test, y_train, y_test = train_test_split(X,y, test_size= 0.25)


for i in range(1,25): 
    knn = KNeighborsClassifier(n_neighbors=i)
    knn.fit(X_train, y_train)

    prediction = knn.predict(X_test)

    perf = accuracy_score(y_test, prediction)

    print(f'parameter {i} had a accuracy score of {perf}')


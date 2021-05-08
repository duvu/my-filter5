import pandas as pd

df = pd.DataFrame({'Age': [20, 21, 22, 24, 32, 38, 39],
                   'Color': ['Blue', 'Green', 'Red', 'White', 'Gray', 'Black',
                             'Red'],
                   'Food': ['Steak', 'Lamb', 'Mango', 'Apple', 'Cheese',
                            'Melon', 'Beans'],
                   'Height': [165, 70, 120, 80, 180, 172, 150],
                   'Score': [4.6, 8.3, 9.0, 3.3, 1.8, 9.5, 2.2],
                   'State': ['NY', 'TX', 'FL', 'AL', 'AK', 'TX', 'TX']
                   },
                  index=['Jane', 'Nick', 'Aaron', 'Penelope', 'Dean',
                         'Christina', 'Cornelia'])
# print("\n -- loc -- \n")
# df.loc[df['Age'] < 40, 'Test'] = 1
#
# print(df)
#
# # print("\n -- iloc -- \n")
# # print(df.iloc[(df['Age'] < 30).values, [1, 3]])


print(df['Age'].shift(-1))
print(df['Age'] < df['Age'].shift(-1))
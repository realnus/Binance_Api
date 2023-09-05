
import pandas as pd
df = pd.read_csv("_TrainData.csv",  sep=',')

#Create Orders DataFrame
df_Orders = pd.DataFrame({
                    'OrderId': pd.Series([], dtype='int'),
                    'OrderName': pd.Series([], dtype='str'),
                    'Symbol': pd.Series([], dtype='str'),
                    'Status': pd.Series([], dtype='str'),
                    'OpenPrice': pd.Series([], dtype='float'),
                    'Quantity': pd.Series([], dtype='float'),
                    'Quantity_Usd': pd.Series([], dtype='float'),
                    'OpenDateTime': pd.Series([], dtype='datetime64[ns]'),
                    'ClosePrice': pd.Series([], dtype='float'),
                    'Profit_Usd': pd.Series([], dtype='float'),
                    'Profit_Percent': pd.Series([], dtype='float'),
                    'CloseDateTime': pd.Series([], dtype='datetime64[ns]')
                  })

#Create Orders DataFrame
df_Levels = pd.DataFrame({
                    'LevelId': pd.Series([], dtype='int'),
                    'Price': pd.Series([], dtype='float')
                    })


IsSearchModeOn = False

LevelZeroPrice = float(0)
LevelPrice = float(0)
DirectionStarted = ""
CycleEndsPrice = float(0)

Buy_Close = float(0)
Sell_Close = float(0)
TargetReached = 0
IsBackToPreviousLevel = False # Bu önemli geriye donus yaptığı zaman alayını kapatmamız lazım. Prev level fiyatına inip inmedidgni ontrl edel,m
PrevLevelPrice = 0 # İkinci level yukarıda ise yon pozitif başladı demektir. Prev level'e gelince kar alınıp bitecek o zaman CurrentPrice =< PrevLevel ile dongu bitecek yada tam tersin
#Loop edelim datayı
for i,item in enumerate(df):
    if(IsSearchModeOn == False):                                                                                                                    
        #Get First Open Value
        LevelZeroPrice = float(df.iloc[i]["Open"])

        #Calculate Next Target Values for 1%
        Buy_Close = LevelZeroPrice + (LevelZeroPrice * 1/100)
        Sell_Close = LevelZeroPrice - (LevelZeroPrice * 1/100)
    
    else:
        #Check Whic one occurs first
        if(df.iloc[i]["Low"] < Sell_Close):
            TargetReached = -1
            LevelPrice = Sell_Close
            DirectionStarted = 'Down'

        if(df.iloc[i]["High"] > Buy_Close):
            TargetReached = 1
            LevelPrice = Buy_Close
            DirectionStarted = "Up"

        if(TargetReached != 0):
            #Close reached Target and Open New ones, keep the wrong direction position Open
            if(DirectionStarted == "Up"):
                #ilk level tamamlanmış demektir.
                LevelPrice = Buy_Close
                Buy_Close = LevelPrice + (LevelPrice * 1/100)
                Sell_Close = LevelPrice - (LevelPrice * 1/100)
                CycleEndsPrice = 



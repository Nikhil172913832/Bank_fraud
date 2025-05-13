import optuna
import numpy as np
from xgboost import XGBClassifier
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import average_precision_score
from sklearn.model_selection import train_test_split
import os
import pandas as pd

df = pd.read_csv("data.csv")
df = pd.get_dummies(df, columns=)

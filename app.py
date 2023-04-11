{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "2870926c",
   "metadata": {},
   "outputs": [],
   "source": [
    "import requests\n",
    "import os\n",
    "import pyspark\n",
    "import matplotlib.pyplot as plt\n",
    "import pyarrow\n",
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.functions import desc\n",
    "import pandas as pd\n",
    "\n",
    "def plot_scatter():\n",
    "    \n",
    "    # create spark session\n",
    "\n",
    "    spark = SparkSession.builder.appName(\"test1\").getOrCreate()\n",
    "\n",
    "    df = spark.read.csv('/stock_dataset.csv', inferSchema=True, sep=',', header=True)\n",
    "    \n",
    "    \n",
    "    df = df.orderBy(desc('Date'))\n",
    "    \n",
    "    df = df.limit(31)\n",
    "\n",
    "    df = df.toPandas()\n",
    "\n",
    "    #visualize last 30 days analysis\n",
    "    \n",
    "    df.plot(x = 'Date', y = ['DPZ', 'AMZN', 'BTC', 'NFLX'])\n",
    "    \n",
    "    \n",
    "plot_scatter()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.10"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}

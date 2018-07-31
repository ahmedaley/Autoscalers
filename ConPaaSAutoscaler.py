#!/usr/bin/env python

'''
Created on Feb 25, 2013

@author: ahmed
'''
#!/usr/bin/env python


import random
import numpy.random as np
import sys
import math,time
import csv
import datetime

import os
import threading
import numpy as nump

from counter import Counter
from prediction_models import Prediction_Models
from performance import StatUtils

class sim(object):

    def __init__(self,name,Server_Speed=22,D=15,Factor=1,R=0,delta_T=5,repair_c=0, Initial_Number_Servers=1):
        self.performance_predictor = Prediction_Models()
        np.seed(0)
        self.name=name
        self.stat_utils = StatUtils()
        random.seed(1)
        self.LessRequests=[]
        self.LessServers=[]
        self.ExtraRequests=[]
        self.ExtraServers=[]
        self.LessRequestsMin=[]
        self.LessServersMin=[]
        self.ExtraRequestsMin=[]
        self.ExtraServersMin=[] 
        self.TotalServers=[]
        self.TotalServersMin=[]
        self.Server_Speed_Avg=Server_Speed
        self.AvgServer_Speed=Server_Speed
        self.Server_Speed=Server_Speed
        self.Factor=Factor
        self.D=D
        TraceFile=open("%s"%(name).strip(),'r')
        self.Trace=TraceFile.readlines()
        self.R=R
        self.delta_T=delta_T
        self.AvgCapacity=Initial_Number_Servers
        self.repair_c=repair_c            #estimates
        self.CurrentCapacity=Initial_Number_Servers
        self.Predicted=0
        self.Time_lastEstimation=0
#         self.outTime=open('./ConPaaSHrWikiResults/ConPaaSGen2%sSS%sD%sF%sT%s.txt'%(name.strip().split("/")[-1],Server_Speed,D,Factor,delta_T),'w')
#         self.outStat=open('./ConPaaSHrWikiResults/StatConPaaSGen2%sSS%sD%sF%sT%s.txt'%(name.strip().split("/")[-1],Server_Speed,D,Factor,delta_T),'w')
#         self.outFuture=open('./ConPaaSHrWikiResults/FutureConPaaSGen2%sSS%sD%sF%sT%s.txt'%(name.strip().split("/")[-1],Server_Speed,D,Factor,delta_T),'w')
        self.lastTime=0
        self.Second_Previous=0
        self.Minute_Previous=0
        self.Hour_Previous=0
        self.NumberOfReactiveServers=0
        self.delta_Hours=0
        self.delta_minutes=0
        self.delta_seconds=0
        self.sigma_Alive=1
        self.Current_Load=0
        self.avg_n=1
        self.u_estimate=0
        self.P_estimate=0
        self.Previous_Load=0
        self.s=0
        np.seed(0)
        self.NumberOfProactiveDecisions=0
        self.numberOfestimations=0
        self.numberReactiveDecisions=0
        self.PastMinute=0
        self.FutureMinute=0
        self.Current_Hour=0
        self.ConPaaS={}
        self.Buffer=0
        self.BufferedLoad=0
        self.Performance=[Server_Speed]
        self.JobsList=[]
        self.SumOptimal=0.0
        self.SumActual=0.0
        self.SumExtra=0.0
        self.SumLess=0.0
        self.SumOptimalQueue=0.0
        self.SumDeployed=0.0
        self.TimeAgg=0
        self.Load=[]
        self.Capacity=[]
        self.lookAhead=0.0
        self.Delta_Load=0
        
        self.forecast_req_rate_model_selected = 0
        self.forecast_req_rate_predicted = 0
        self.forecast_list_req_rate = {}
        
        with open("./SlowerConPaaSHrWikiResults/consiceResults%s.csv"%(name.strip().split("/")[-1]), "wb") as csv_file:    
            self.writer=csv.writer(csv_file,delimiter=',')
            self.writer.writerow(["Current_Time","CapRequiredServers", "CurrentCapacity","RunningJobs","Average Server Speed","Load_Requests","Buffer","Current_Load", "Past Minute", "FutureMinute","Calculation Time"])
#         
        

    def csv_writer(self,data, path):
        """
        Write data to a CSV file path
        """
        with open(path, "wb") as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            for line in data:
                writer.writerow(line)    
                
    def run_sim(self):
        x=self.simloop()
        return x
    
    
    def prediction_evaluation(self, req_rate_data):
            data_req_rate_filtered = req_rate_data
           
            async_result_req_ar = self.performance_predictor.auto_regression(data_req_rate_filtered, 20)
            async_result_req_lr = self.performance_predictor.linear_regression(data_req_rate_filtered, 20)
            async_result_req_exp_smoothing = self.performance_predictor.exponential_smoothing(data_req_rate_filtered, 2)

            self.forecast_list_req_rate[1] = async_result_req_lr
            self.forecast_list_req_rate[2] = async_result_req_exp_smoothing
            self.forecast_list_req_rate[0] = async_result_req_ar
            #  self.forecast_list_req_rate[0] = async_result_req_arma.get()


#             try:
#             print "Getting the forecast request rate for the best model in the previous iteration " + str(self.forecast_req_rate_model_selected)
            weight_avg_predictions = self.stat_utils.compute_weight_average(self.forecast_list_req_rate[self.forecast_req_rate_model_selected])

            if weight_avg_predictions > 0:
                self.forecast_req_rate_predicted = weight_avg_predictions
            
#             print "Prediction request rate for model " + str(self.forecast_req_rate_model_selected) + "--  Prediction req. rate: " + str(self.forecast_req_rate_predicted)
            return weight_avg_predictions
#             except Exception as e:
#                 print "Warning trying to predict a future value for the model." + str(e)

        

    def get_req_rate_prediction(self):
        return self.forecast_req_rate_predicted
  
   
    def simloop(self):
        GammaTime=0
        self.PreviousCapacity=0
        Time=0
        self.Previous_Load=0
        self.Time_Previous=float(self.Trace[0].split()[1])
        initial_Time=self.Time_Previous
        self.PastMinute=float(self.Trace[0].split()[2])*self.Factor
        self.delta_Time=1
        GammaTime=self.Time_Previous
        self.PastMinute=1#math.ceil(self.PastMinute/self.Server_Speed)*3
        Delta_Load=0
        CapacityList=[]  
        LoadServers=[]
        for t in self.Trace:
            #print self.JobsList
            self.Server_Speed=np.poisson(self.Server_Speed_Avg) #std deviation for 1000 requests =50
            t=t.split()
            Current_Time=self.Time_Previous+1#float(t[1])
            self.Current_Load=float(t[2])
            self.CurrentCapacity=self.Predicted
           # self.LoadCalculator2()
#            if self.CurrentCapacity*self.Server_Speed> sum(self.JobsList)+self.Current_Load+self.Buffer:
 #               self.JobsList[-1]=self.Current_Load+self.Buffer
  #              self.Buffer=0
   #         else:
    #            self.Buffer+=(sum(self.JobsList)+self.Current_Load-(self.CurrentCapacity*self.Server_Speed))
     #           if self.CurrentCapacity*self.Server_Speed-sum(self.JobsList[0:-1])>0:
      #              self.JobsList[-1]=self.CurrentCapacity*self.Server_Speed-sum(self.JobsList[0:-1])
            LoadServers+=[int(math.ceil(float(self.Current_Load)))]
#             print Load
            #print Load
            self.delta_Time=1
            self.Time_Previous=Current_Time          
            x=Current_Time-self.Time_lastEstimation           
            tempCapac=0
            t1=time.time()
            if len(LoadServers)<21:
                self.predicted=int(math.ceil(float(self.Current_Load))/self.Server_Speed)
            else:
                if self.Current_Load>self.Server_Speed:
                    self.Predicted=self.prediction_evaluation(LoadServers)
                    if self.Predicted==0 or self.Predicted==None:
                        self.Predicted=self.CurrentCapacity
                else:
                    self.Predicted=self.Server_Speed/3
                LoadServers.pop(0)
            
                
                
            
            
            t2=time.time()
#             print>>self.outTime, t2-t1
#             self.Load+=[LoadServers]
#             self.Capacity+=[self.CurrentCapacity]
#             self.TimeAgg+=1
#             self.SumOptimal+=self.Current_Load/self.Server_Speed
#             self.SumOptimalQueue+=LoadServers
#             self.SumActual+=self.CurrentCapacity
# #             self.numberOfestimations+=abs(self.PastMinute)            
            with open("./SlowerConPaaSHrWikiResults/consiceResults%s.csv"%(self.name.strip().split("/")[-1]), "a") as csv_file:
                    self.writer=csv.writer(csv_file,delimiter=',')    
                    self.writer.writerow([t[0],int(math.ceil(float(self.Current_Load)/self.Server_Speed)),int(math.ceil(float(self.CurrentCapacity)/self.Server_Speed)),sum(self.JobsList),self.Server_Speed,self.Current_Load,self.Buffer,self.Current_Load, self.PastMinute,self.FutureMinute, t2-t1])            
#         for i in range(len(self.Capacity)):
#             try:
#                 FutureLoad=self.Load[i:i+10]
#             except:
#                 FutureLoad=self.Load[i:]
#             MAX=max(FutureLoad)
#             if self.Capacity[i]-MAX>0: 
#                 self.SumExtra+=self.Capacity[i]-MAX
#                 self.lookAhead+=FutureLoad.index(MAX)
#                 print>>self.outFuture, MAX, self.SumExtra, self.SumLess
#             elif self.Capacity[i]-self.Load[i]<0:
#                 self.SumLess+=self.Capacity[i]-self.Load[i]
#                 print>>self.outFuture, "Less",  MAX, self.SumExtra, self.SumLess
#             else:
#                 pass
#         print>>self.outStat,'SumOptimal',self.SumOptimal,'AvgOptimal',self.SumOptimal/self.TimeAgg,
#         print >>self.outStat,'SumOptimalQueue',self.SumOptimalQueue,'AvgOptimalQueue',self.SumOptimalQueue/self.TimeAgg,'SumExtra',
#         print>>self.outStat, self.SumExtra,'AvgExtra',self.SumExtra/self.TimeAgg,'SumLess',self.SumLess,'AvgLess',self.SumLess/self.TimeAgg, 'ActualAvg',
#         print>>self.outStat, self.SumActual/self.TimeAgg, " Horizon ",self.lookAhead/self.TimeAgg, "NumberOfChanges",self.numberOfestimations,"AvgNumberOfChanges",self.numberOfestimations/self.TimeAgg
#                             
        self.outStat.close()
        self.outTime.close()

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

class sim(object):

    def __init__(self,name,Server_Speed=1000.0,D=5,Factor=1,R=0,delta_T=5,repair_c=0, Initial_Number_Servers=1):
        np.seed(0)
        self.name=name
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
        self.D=D
        TraceFile=open("%s"%(name).strip(),'r')
        self.AvgServer_Speed=Server_Speed
        self.Server_Speed=Server_Speed
        self.Factor=Factor
        self.Trace=TraceFile.readlines()
        self.delta_T=delta_T
        self.CurrentCapacity=Initial_Number_Servers
        self.Time_lastEstimation=0
        self.outTime=open('./SlowerReactHrWikiResults/ReactiveGen2%sSS%sD%sF%sT%s.txt'%(name.strip().split("/")[-1],Server_Speed,D,Factor,delta_T),'w')
        self.outStat=open('./SlowerReactHrWikiResults/StatReactiveGen2%sSS%sD%sF%sT%s.txt'%(name.strip().split("/")[-1],Server_Speed,D,Factor,delta_T),'w')
        self.outFuture=open('./SlowerReactHrWikiResults/FutureReactiveGen2%sSS%sD%sF%sT%s.txt'%(name.strip().split("/")[-1],Server_Speed,D,Factor,delta_T),'w')
        self.lastTime=0
        self.NumberOfReactiveServers=0
        self.sigma_Alive=1
        self.Current_Load=0
        self.avg_n=1
        self.Previous_Load=0
        self.s=0
        self.NumberOfProactiveDecisions=0
        self.numberOfestimations=0
        self.numberReactiveDecisions=0
        self.PastMinute=0
        self.FutureMinute=0
        self.Current_Hour=0
        self.histogram={}
        for i in range(24):
            self.histogram[i]=[]
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
        self.ErrorPasthrs=[]
        with open("./SlowerReactHrWikiResults/consiceResults%s.csv"%(name.strip().split("/")[-1]), "wb") as csv_file:    
            self.writer=csv.writer(csv_file,delimiter=',')
            self.writer.writerow(["Current_Time","CapRequiredServers", "CurrentCapacity","RunningJobs","Average Server Speed","Load","Buffer","Current_Load", "Past Minute", "FutureMinute","Calculation Time"])
    
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

    
    def repair(self,Load):
        TemporaryVariable=math.ceil(Load/self.Server_Speed)-self.CurrentCapacity
        if TemporaryVariable>0 :
            self.PastMinute=self.CurrentCapacity+int(TemporaryVariable*1.2)
#             print Load, self.FutureMinute, self.FutureMinute*5
            self.NumberOfReactiveServers+=TemporaryVariable
            self.numberReactiveDecisions+=abs(TemporaryVariable)           
        elif 1.1*float(Load)/self.Server_Speed<self.CurrentCapacity :
            self.PastMinute=math.ceil(float(Load)/self.Server_Speed)*1.2         #because when I scale down, no more requests go to the VMs to shut down2
            self.NumberOfReactiveServers-=(TemporaryVariable+1)
            self.numberReactiveDecisions+=abs(TemporaryVariable)
        else:
            return    
                          
    def simloop(self):
        GammaTime=0
        self.PreviousCapacity=0
        Time=0
        self.Previous_Load=0
        self.Time_Previous=float(self.Trace[0].split()[1])
        self.PastMinute=float(self.Trace[0].split()[2])*self.Factor
        self.delta_Time=1
        self.PastMinute=1#math.ceil(self.PastMinute/self.Server_Speed)*3
        Delta_Load=0  
        
        for t in self.Trace:
            #print self.JobsList
            self.Server_Speed=np.poisson(self.Server_Speed_Avg) #std deviation for 1000 requests =50
            t=t.split()
            Current_Time=self.Time_Previous+1#float(t[1])
            self.CurrentCapacity=self.PastMinute
            self.Current_Load=float(t[2])
           # self.LoadCalculator2()
#            if self.CurrentCapacity*self.Server_Speed> sum(self.JobsList)+self.Current_Load+self.Buffer:
 #               self.JobsList[-1]=self.Current_Load+self.Buffer
  #              self.Buffer=0
   #         else:
    #            self.Buffer+=(sum(self.JobsList)+self.Current_Load-(self.CurrentCapacity*self.Server_Speed))
     #           if self.CurrentCapacity*self.Server_Speed-sum(self.JobsList[0:-1])>0:
      #              self.JobsList[-1]=self.CurrentCapacity*self.Server_Speed-sum(self.JobsList[0:-1])
            Load=self.Current_Load
            LoadServers=math.ceil(Load/self.Server_Speed) 
            self.ErrorPasthrs+=[self.CurrentCapacity-LoadServers]   
            self.delta_Time=1#(Current_Time-self.Time_Previous)/3600
            self.Time_Previous=Current_Time   
#            Controller Code starts here       
            t1=time.time()
            self.repair(Load)
            self.sigma_Alive+=self.CurrentCapacity
            t2=time.time()
            self.Load+=[math.ceil(float(Load)/self.Server_Speed)]
            self.Capacity+=[self.CurrentCapacity]
            self.TimeAgg+=1
            self.SumOptimal+=self.Current_Load/self.Server_Speed
            self.SumOptimalQueue+=math.ceil(float(Load)/self.Server_Speed)
            self.SumActual+=self.CurrentCapacity
            self.numberOfestimations+=abs(self.CurrentCapacity-self.PastMinute)
            with open("./SlowerReactHrWikiResults/consiceResults%s.csv"%(self.name.strip().split("/")[-1]), "a") as csv_file:
                    self.writer=csv.writer(csv_file,delimiter=',')    
                    self.writer.writerow([t[0],int(math.ceil(float(Load)/self.Server_Speed)),self.CurrentCapacity,sum(self.JobsList),self.Server_Speed,Load,self.Buffer,self.Current_Load, self.PastMinute,self.FutureMinute, t2-t1])
            
        for i in range(len(self.Capacity)):
            try:
                FutureLoad=self.Load[i:i+10]
            except:
                FutureLoad=self.Load[i:]
            MAX=max(FutureLoad)
            if self.Capacity[i]-MAX>0 : 
                self.SumExtra+=self.Capacity[i]-MAX
                self.lookAhead+=FutureLoad.index(MAX)
                print>>self.outFuture, MAX
            elif self.Capacity[i]-self.Load[i]<0:
                self.SumLess+=self.Capacity[i]-self.Load[i]
            else:
                pass                
        print>>self.outStat,'SumOptimal',self.SumOptimal,'AvgOptimal',self.SumOptimal/self.TimeAgg,
        print >>self.outStat,'SumOptimalQueue',self.SumOptimalQueue,'AvgOptimalQueue',self.SumOptimalQueue/self.TimeAgg,'SumExtra',
        print>>self.outStat, self.SumExtra,'AvgExtra',self.SumExtra/self.TimeAgg,'SumLess',self.SumLess,'AvgLess',self.SumLess/self.TimeAgg, 'ActualAvg',
        print>>self.outStat, self.SumActual/self.TimeAgg, " Horizon ",self.lookAhead/self.TimeAgg, "NumberOfChanges",self.numberOfestimations,"AvgNumberOfChanges",self.numberOfestimations/self.TimeAgg
                            
        self.outStat.close()
        self.outTime.close()

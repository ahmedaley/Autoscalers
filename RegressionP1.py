#!/usr/bin/env python


import random
import numpy.random as np
import sys
import math,time,numpy
import csv


class sim(object):

    def __init__(self,name,Server_Speed=1000.0,D=5,Factor=1,R=0,delta_T=5,repair_c=0, Initial_Number_Servers=1):
        
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
        TraceFile=open("./GeneratedWorkloads/%s"%(name).strip(),'r')
        self.AvgServer_Speed=Server_Speed
        self.Server_Speed=Server_Speed
        self.Factor=Factor
        self.D=D
        self.Trace=TraceFile.readlines()
        self.R=R
        self.delta_T=delta_T
        self.AvgCapacity=Initial_Number_Servers
        self.repair_c=repair_c            #estimates
        self.CurrentCapacity=Initial_Number_Servers
        self.Time_lastEstimation=0
        self.outTime=open('./GeneratedWorkloadsResults/RegressionP1Gen2%sSS%sD%sF%sT%s.txt'%(name.strip(),Server_Speed,D,Factor,delta_T),'w')
        self.outStat=open('./GeneratedWorkloadsResults/StatRegressionP1Gen2%sSS%sD%sF%sT%s.txt'%(name.strip(),Server_Speed,D,Factor,delta_T),'w')
        self.outFuture=open('./GeneratedWorkloadsResults/FutureRegressionP1Gen2%sSS%sD%sF%sT%s.txt'%(name.strip(),Server_Speed,D,Factor,delta_T),'w')
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
        self.NumberOfProactiveDecisions=0
        self.numberOfestimations=0
        self.numberReactiveDecisions=0
        self.PastMinute=0
        self.FutureMinute=0
        self.Current_Hour=0
        self.histogram={}
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
        self.RegressionListX=[]
        self.RegressionProactiveLoad=[]
        self.p=0
        self.provCost=0
        np.seed(0)
        #==========================================================================================================
        # End of C_new variables    

    def run_sim(self):
        x=self.simloop()
        return x

    
    #Adding the Estimator and the controller functionality

    def estimator(self,Time):
#        variance=numpy.std(self.RegressionProactiveLoad)
        if math.ceil(self.p(Time))>0:
#            r=random.normalvariate(0,variance)
            self.PastMinute=math.ceil(self.p(Time))
#            print>>self.outTime,"ah",self.PastMinute,self.CurrentCapacity,self.p(Time)
            if self.PastMinute>self.CurrentCapacity:
                self.PastMinute=self.CurrentCapacity
            elif self.PastMinute>0:
                pass
            else:
                self.PastMinute+=self.CurrentCapacity 
        

     
    def repair(self,Current_Time,Load):
        TemporaryVariable=math.ceil((Load/self.Server_Speed)*1.05)
#        print>>self.outTime,"heba", Load, self.Server_Speed
        self.FutureMinute=TemporaryVariable
        if float(Load)/self.Server_Speed>self.CurrentCapacity and (Load-self.CurrentCapacity*self.Server_Speed)>self.D :
            self.PastMinute=self.FutureMinute
            self.NumberOfReactiveServers+=TemporaryVariable
            self.numberReactiveDecisions+=abs(TemporaryVariable)   
            self.NumberOfReactiveServers+=TemporaryVariable
            self.numberReactiveDecisions+=1
             
    def LoadCalculator(self):
        try:
            RunningJobsTimes=len(self.JobsList)
            if RunningJobsTimes==60:
                self.JobsList.pop(0)
            elif  RunningJobsTimes>0:
                self.JobsList[-2]-=math.ceil(self.JobsList[-2]*0.2)
                self.JobsList[-3]-=math.ceil(self.JobsList[-3]*0.15)
                self.JobsList[-4]-=math.ceil(self.JobsList[-4]*0.207)
                self.JobsList[-7]-=math.ceil(self.JobsList[-6]*0.18)
                self.JobsList[-12]-=math.ceil(self.JobsList[-12]*0.19)
                self.JobsList[-21]-=math.ceil(self.JobsList[-21]*0.23)
                self.JobsList[-31]-=math.ceil(self.JobsList[-31]*0.29)
        except:
            pass
        
    def LoadCalculator2(self):
        try:
            RunningJobsTimes=len(self.JobsList)
            if RunningJobsTimes==3:
                self.JobsList.pop(0)
            elif  RunningJobsTimes>0:
                self.JobsList[-2]-=math.ceil(self.JobsList[-2])
#                self.JobsList[-3]-=math.ceil(self.JobsList[-3]*0.15)
#                self.JobsList[-4]-=math.ceil(self.JobsList[-4]*0.207)
#                self.JobsList[-7]-=math.ceil(self.JobsList[-6]*0.18)
#                self.JobsList[-12]-=math.ceil(self.JobsList[-12]*0.19)
#                self.JobsList[-21]-=math.ceil(self.JobsList[-21]*0.23)
#                self.JobsList[-31]-=math.ceil(self.JobsList[-31]*0.29)
        except:
            pass
        
    
    def LoadCalculator3(self):
        try:
            RunningJobsTimes=len(self.JobsList)
            if RunningJobsTimes==6:
                self.JobsList.pop(0)
            elif  RunningJobsTimes>0:
                self.JobsList[-2]-=math.ceil(self.JobsList[-2]*0.7)
                self.JobsList[-3]-=math.ceil(self.JobsList[-3]*0.7)
                self.JobsList[-4]-=math.ceil(self.JobsList[-4]*0.9)
        except:
            pass
    
    def simloop(self):
        GammaTime=0
        self.PreviousCapacity=0
        Time=0
        self.Previous_Load=0
        self.Time_Previous=int(self.Trace[0].split()[0])
        self.Previous_Load=float(self.Trace[0].split()[1])*self.Factor
        self.delta_Time=Time
        CapacityList=[]
        self.CurrentCapacity=math.ceil(self.Previous_Load/self.Server_Speed)
        Delta_Load=0  
        
        for t in self.Trace:
            #print self.JobsList
            self.Server_Speed=np.poisson(self.Server_Speed_Avg) #std deviation for 1000 requests =50
            t=t.split()
            Current_Time=int(t[0])
            self.Current_Load=float(t[1])  
            self.CurrentCapacity=self.PastMinute            
            self.JobsList+=[0]
            self.LoadCalculator2()
            #print "CC", self.CurrentCapacity
            if self.CurrentCapacity*self.Server_Speed> sum(self.JobsList)+self.Current_Load+self.Buffer:
                self.JobsList[-1]=self.Current_Load+self.Buffer
                self.Buffer=0
            else:
                self.Buffer+=(sum(self.JobsList)+self.Current_Load-(self.CurrentCapacity*self.Server_Speed))
                if self.CurrentCapacity*self.Server_Speed-sum(self.JobsList[0:-1])>0:
                    self.JobsList[-1]=self.CurrentCapacity*self.Server_Speed-sum(self.JobsList[0:-1])
                    
                #print self.JobsList
            Load=sum(self.JobsList)+self.Buffer
            #print Load
            self.delta_Time=Current_Time-self.Time_Previous
            self.Time_Previous=Current_Time          
            x=Current_Time-self.Time_lastEstimation           
            CapacityList+=[(self.CurrentCapacity,self.delta_Time)]
            tempCapac=0
            Time=Current_Time
            t1=time.time()
            self.RegressionProactiveLoad+=[math.ceil(float(Load)/self.Server_Speed)] 
            self.RegressionListX+=[Time]
            if (self.CurrentCapacity>math.ceil(float(Load)/self.Server_Speed)):
                self.p = numpy.poly1d(numpy.polyfit(self.RegressionListX[-60:], self.RegressionProactiveLoad[-60:], deg=2))
                self.estimator(Time)
            else:
                self.repair(Current_Time,Load)
            t2=time.time()
            self.Load+=[math.ceil(float(Load)/self.Server_Speed)]
            self.Capacity+=[self.CurrentCapacity]
            self.TimeAgg+=1
            self.SumOptimal+=self.Current_Load/self.Server_Speed
            self.SumOptimalQueue+=math.ceil(float(Load)/self.Server_Speed)
            self.SumActual+=self.CurrentCapacity
            self.numberOfestimations+=abs(self.CurrentCapacity-self.PastMinute)
            print>>self.outTime, Current_Time,"CapRequiredServers",math.ceil(float(Load)/self.Server_Speed),"CapSer",self.CurrentCapacity,"RunningJobs",sum(self.JobsList),"SS",self.Server_Speed,"L",Load,"Buffer",self.Buffer,"LM",self.Current_Load, "PM", self.PastMinute,"FM",self.FutureMinute,"D",self.delta_T,"avC",self.AvgCapacity," P",self.P_estimate,"DL",Delta_Load,"u", self.u_estimate, "P_est", self.P_estimate, "Avg_n",self.avg_n ,"R",self.R,"Add/Remove",self.repair_c,"pro ",self.NumberOfProactiveDecisions,"NE",self.numberOfestimations,"NRD",self.numberReactiveDecisions,"Calc.Time", t2-t1
            
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

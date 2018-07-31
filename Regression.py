#!/usr/bin/env python


import random
import numpy.random as np
import sys
import csv
import math,time,numpy

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
        TraceFile=open("%s"%(name).strip(),'r')
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
        self.outTime=open('./SlowerRegHrWikiResults/RegressionGen2%sSS%sD%sF%sT%s.txt'%(name.strip().split("/")[-1],Server_Speed,D,Factor,delta_T),'w')
        self.outStat=open('./SlowerRegHrWikiResults/StatRegressionGen2%sSS%sD%sF%sT%s.txt'%(name.strip().split("/")[-1],Server_Speed,D,Factor,delta_T),'w')
        self.outFuture=open('./SlowerRegHrWikiResults/FutureRegressionGen2%sSS%sD%sF%sT%s.txt'%(name.strip().split("/")[-1],Server_Speed,D,Factor,delta_T),'w')
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
        with open("./SlowerRegHrWikiResults/consiceResults%s.csv"%(name.strip().split("/")[-1]), "wb") as csv_file:    
            self.writer=csv.writer(csv_file,delimiter=',')
            self.writer.writerow(["Current_Time","CapRequiredServers", "CurrentCapacity","RunningJobs","Average Server Speed","Load_Requests","Buffer","Current_Load", "Past Minute", "FutureMinute","Calculation Time"])
    
    def csv_writer(self,data, path):
        """
        Write data to a CSV file path
        """
        with open(path, "wb") as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            for line in data:
                writer.writerow(line)    
           
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
            self.PastMinute=math.ceil(self.p(Time))*1.2
#            print>>self.outTime,"ah",self.PastMinute,self.CurrentCapacity,self.p(Time)
            if self.PastMinute>self.CurrentCapacity:
                self.PastMinute=self.CurrentCapacity
            elif self.PastMinute>0:
                pass
            else:
                self.PastMinute+=self.CurrentCapacity 
        

     
    def repair(self,Current_Time,Load):
        TemporaryVariable=math.ceil((Load/self.Server_Speed)*1.05)
        self.FutureMinute=TemporaryVariable
        if float(Load)/self.Server_Speed>self.CurrentCapacity and (Load-self.CurrentCapacity*self.Server_Speed)>self.D :
            self.PastMinute=self.FutureMinute
            self.NumberOfReactiveServers+=TemporaryVariable
            self.numberReactiveDecisions+=abs(TemporaryVariable)   
            self.NumberOfReactiveServers+=TemporaryVariable
            self.numberReactiveDecisions+=1
             
    
    
    def simloop(self):
        self.PreviousCapacity=0
        Time=0
        self.Time_Previous=int(self.Trace[0].split()[0])
        self.Previous_Load=float(self.Trace[0].split()[2])*self.Factor
        self.delta_Time=Time
        CapacityList=[]
        self.CurrentCapacity=math.ceil(self.Previous_Load/self.Server_Speed)
        Delta_Load=0  
        
        for t in self.Trace:
            #print self.JobsList
            self.Server_Speed=np.poisson(self.Server_Speed_Avg) #std deviation for 1000 requests =50
            t=t.split()
            Current_Time=self.Time_Previous+1
            self.Current_Load=float(t[2])  
            self.CurrentCapacity=self.PastMinute            
            #print "CC", self.CurrentCapacity
            
            Load=self.Current_Load
            #print Load
            self.delta_Time=1
            self.Time_Previous=Current_Time          
            CapacityList+=[(self.CurrentCapacity,self.delta_Time)]
            Time=Current_Time
            t1=time.time()
            LoadServers=math.ceil(float(Load)/self.Server_Speed)
            self.RegressionProactiveLoad+=[LoadServers] 
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
            with open("./SlowerRegHrWikiResults/consiceResults%s.csv"%(self.name.strip().split("/")[-1]), "a") as csv_file:
                    self.writer=csv.writer(csv_file,delimiter=',')    
                    self.writer.writerow([t[0],LoadServers,self.CurrentCapacity,sum(self.JobsList),self.Server_Speed,self.Current_Load,self.Buffer,self.Current_Load, self.PastMinute,self.FutureMinute, t2-t1])
            
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

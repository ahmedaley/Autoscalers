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

class sim(object):

    def __init__(self,name,Server_Speed=1000.0,D=15,Factor=1,R=0,delta_T=5,repair_c=0, Initial_Number_Servers=1):
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
        self.Time_lastEstimation=0
        self.outTime=open('./SlowerAKTEHrWikiResults/AKTEGen2%sSS%sD%sF%sT%s.txt'%(name.strip().split("/")[-1],Server_Speed,D,Factor,delta_T),'w')
        self.outStat=open('./SlowerAKTEHrWikiResults/StatAKTEGen2%sSS%sD%sF%sT%s.txt'%(name.strip().split("/")[-1],Server_Speed,D,Factor,delta_T),'w')
        self.outFuture=open('./SlowerAKTEHrWikiResults/FutureAKTEGen2%sSS%sD%sF%sT%s.txt'%(name.strip().split("/")[-1],Server_Speed,D,Factor,delta_T),'w')
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
        self.AKTE={}
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
        with open("./SlowerAKTEHrWikiResults/consiceResults%s.csv"%(name.strip().split("/")[-1]), "wb") as csv_file:    
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
                
    def run_sim(self):
        x=self.simloop()
        return x
#Adding the Estimator and the controller functionality

   #Adding the Estimator and the controller functionality

    def estimator(self,t,Delta_Load):
        self.Delta_Load=Delta_Load
        self.avg_n=float(self.sigma_Alive)/t#self.delta_T #t   
        self.u_estimate=float(self.AvgCapacity)/self.avg_n
        self.P_estimate=float(Delta_Load)/self.avg_n#tmath.ceil(self.delta_T) #math.ceil(self.delta_T) #
        self.delta_T=float(self.D*5)/self.AvgCapacity
        self.Performance.sort()
       # print len(self.Performance)/2

    
    
    def controller(self):
            R=self.u_estimate*self.P_estimate*self.avg_n
            if R<0:
                self.R=R/4
                print "Neg", self.R/2
            else:
                self.R=R
                
            

     
    def repair(self,Load):
        self.repair_c+=(self.R*self.delta_Time)
        Proactive=0
        self.PastMinute=0
        if  self.repair_c<0:    
            self.s=int(self.repair_c)
            self.repair_c-=self.s    
            if self.CurrentCapacity+self.s>math.ceil(self.Current_Load/self.Server_Speed):
                self.CurrentCapacity+=math.ceil(self.s)   #because when I scale down, no more requests go to the VMs to shut down.
                self.NumberOfProactiveDecisions+=abs(self.s)
            else:
                self.CurrentCapacity=math.ceil(self.Current_Load/self.Server_Speed)
                return
        elif self.repair_c>=1:
            Proactive=int(self.repair_c)
            self.repair_c-=Proactive  
            self.NumberOfProactiveDecisions+=abs(Proactive) 
        
        TemporaryVariable=math.ceil(1.1*(Load))-self.CurrentCapacity
            
        if float(Load)>self.CurrentCapacity:
            self.FutureMinute=TemporaryVariable+Proactive
            self.PastMinute=math.ceil(self.FutureMinute)
            self.NumberOfReactiveServers+=TemporaryVariable
            self.numberReactiveDecisions+=abs(TemporaryVariable)
        print self.name, self.CurrentCapacity, self.R, self.delta_Time, self.AvgCapacity, self.u_estimate,  self.Delta_Load, self.sigma_Alive, self.avg_n
             
    
             
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
        
        for t in self.Trace:
            self.D=0.01*self.CurrentCapacity
            #print self.JobsList
            self.Server_Speed=np.poisson(self.Server_Speed_Avg) #std deviation for 1000 requests =50
            t=t.split()
            Current_Time=self.Time_Previous+1#float(t[1])
            self.CurrentCapacity+=self.PastMinute
            self.Current_Load=float(t[2])
           # self.LoadCalculator2()
#            if self.CurrentCapacity*self.Server_Speed> sum(self.JobsList)+self.Current_Load+self.Buffer:
 #               self.JobsList[-1]=self.Current_Load+self.Buffer
  #              self.Buffer=0
   #         else:
    #            self.Buffer+=(sum(self.JobsList)+self.Current_Load-(self.CurrentCapacity*self.Server_Speed))
     #           if self.CurrentCapacity*self.Server_Speed-sum(self.JobsList[0:-1])>0:
      #              self.JobsList[-1]=self.CurrentCapacity*self.Server_Speed-sum(self.JobsList[0:-1])
            LoadServers=int(math.ceil(float(self.Current_Load)/self.Server_Speed))
#             print Load
            #print Load
            self.delta_Time=1
            self.Time_Previous=Current_Time          
            x=Current_Time-self.Time_lastEstimation           
            CapacityList+=[(self.CurrentCapacity,self.delta_Time)]
            tempCapac=0
            t1=time.time()
            print self.delta_T, Current_Time-GammaTime
            if Current_Time-GammaTime>=math.ceil(self.delta_T):   # To be revised, Why 500 !
                for i in CapacityList:
#                        print i[0],i[1],self.AvgCapacity,Current_Time,GammaTime
                        tempCapac+=i[0]*i[1]
                        self.AvgCapacity=float(tempCapac)/(Current_Time-GammaTime)   
                self.PreviousCapacity=self.CurrentCapacity
                GammaTime=Current_Time
                CapacityList=[]
            if x>=self.delta_T:  
                print "Load", LoadServers, "Capacity",self.CurrentCapacity  
                Delta_Load=LoadServers-self.CurrentCapacity
                t_calc=Current_Time-initial_Time
                self.estimator(t_calc,Delta_Load)
                self.controller()        
                self.Time_lastEstimation=Current_Time
                self.Previous_Load=self.Current_Load
            self.repair(LoadServers)
            self.sigma_Alive+=self.CurrentCapacity
            t2=time.time()
            self.Load+=[LoadServers]
            self.Capacity+=[self.CurrentCapacity]
            self.TimeAgg+=1
            self.SumOptimal+=self.Current_Load/self.Server_Speed
            self.SumOptimalQueue+=LoadServers
            self.SumActual+=self.CurrentCapacity
            self.numberOfestimations+=abs(self.PastMinute)            
            with open("./SlowerAKTEHrWikiResults/consiceResults%s.csv"%(self.name.strip().split("/")[-1]), "a") as csv_file:
                    self.writer=csv.writer(csv_file,delimiter=',')    
                    self.writer.writerow([t[0],LoadServers,self.CurrentCapacity,sum(self.JobsList),self.Server_Speed,self.Current_Load,self.Buffer,self.Current_Load, self.PastMinute,self.FutureMinute, t2-t1])            
        for i in range(len(self.Capacity)):
            try:
                FutureLoad=self.Load[i:i+10]
            except:
                FutureLoad=self.Load[i:]
            MAX=max(FutureLoad)
            if self.Capacity[i]-MAX>0: 
                self.SumExtra+=self.Capacity[i]-MAX
                self.lookAhead+=FutureLoad.index(MAX)
                print>>self.outFuture, MAX, self.SumExtra, self.SumLess
            elif self.Capacity[i]-self.Load[i]<0:
                self.SumLess+=self.Capacity[i]-self.Load[i]
                print>>self.outFuture, "Less",  MAX, self.SumExtra, self.SumLess
            else:
                pass
        print>>self.outStat,'SumOptimal',self.SumOptimal,'AvgOptimal',self.SumOptimal/self.TimeAgg,
        print >>self.outStat,'SumOptimalQueue',self.SumOptimalQueue,'AvgOptimalQueue',self.SumOptimalQueue/self.TimeAgg,'SumExtra',
        print>>self.outStat, self.SumExtra,'AvgExtra',self.SumExtra/self.TimeAgg,'SumLess',self.SumLess,'AvgLess',self.SumLess/self.TimeAgg, 'ActualAvg',
        print>>self.outStat, self.SumActual/self.TimeAgg, " Horizon ",self.lookAhead/self.TimeAgg, "NumberOfChanges",self.numberOfestimations,"AvgNumberOfChanges",self.numberOfestimations/self.TimeAgg
                            
        self.outStat.close()
        self.outTime.close()

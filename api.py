from flask import *
import numpy as np
import io, base64
import random
import numpy as np
# import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure

'''
Contains all functions used/called in emp_analysis_project.py (app file)
'''

class EmpAnalysis:

    def __init__(self, path):
        '''
        Connects to the directory where the files with raw data reside
        '''
        self.path = path

    def data(self):
        '''
        Import and Clean the Employee file
        '''
        df = pd.read_csv(self.path+"/emp_file_CAPSTONE.txt", names=["first_name", "last_name", "middle_initial", "gender", "s_g", "salary", "dept", "hire_date", "term_date"], index_col=False)
        df.fillna("", inplace=True)
        df.drop([0], inplace=True)
        for col in df.columns:
            if col == "hire_date" or col == "term_date":
                df[col] = pd.to_datetime(df[col], errors="ignore", infer_datetime_format=True)
            else:
                df[col] = df[col].apply(lambda x: "".join([i for i in x if ord(i.lower()) in range(97, 123) or ord(i) in range(48, 58)]))
                if col == "last_name" or col == "dept":
                    df[col] = df[col].apply(lambda x: x.upper())
                elif col == "salary":
                    df[col] = df[col].apply(lambda x: "".join([str((int(i)%10+7)%10) if x.startswith("3X") else i for i in x.replace("3X", "")]))
        df["hire_date"] = df["hire_date"].dt.date

        # Create a column "Name" with LAST_name, First_name, and Middle initial.
        df["name"] = [df.loc[n, "last_name"]+", "+df.loc[n, "first_name"]+", "+df.loc[n, "middle_initial"]+"." if df.loc[n, "middle_initial"] else df.loc[n, "last_name"]+", "+df.loc[n, "first_name"] for n in df.index]
        df.fillna("", inplace=True)
        return df
    
    def emp_list_df(self):
        '''
        Create an alphabetic list of employees by name
        '''
        df = self.data()
        res = df[["name", "gender", "s_g", "salary", "dept", "hire_date", "term_date"]].sort_values(by=["name"], ascending=True)
        return res
    
    
    def emp_counts_sg_df(self):
        '''
        Create a DataFrame for each Salary Grade with Total Count of Employees by Gender
        '''
        df = self.emp_list_df()
        grades = df.s_g.unique()
        grades.sort()
        emp_counts_sg = pd.DataFrame(index=grades, columns=["Female", "Male"])
        emp_counts_sg.index.name="salary grade"
        for grade in grades:
            female = df[(df["s_g"] == grade)&(df["gender"] == "F")]["salary"].count()
            male = df[(df["s_g"] == grade)&(df["gender"] == "M")]["salary"].count()
            emp_counts_sg.loc[grade] = [female, male]
        return emp_counts_sg
    
        
    def emp_counts_dept_df(self):
        '''
        Create a DataFrame for each Salary Grade with Total Count of Employees by Dept
        '''
        df = self.emp_list_df()
        departments = df.dept.unique()
        departments.sort()
        emp_counts_dept = pd.DataFrame(index=departments, columns=["Female", "Male"])
        emp_counts_dept.index.name="dept"
        for dept in departments:
            female = len(df[(df["dept"] == dept)&(df["gender"] == "F")].index)
            male = len(df[(df["dept"] == dept)&(df["gender"] == "M")].index)
            emp_counts_dept.loc[dept] = [female, male]
        return emp_counts_dept

    
    def dept_file_df(self):
        '''
        Import and Clean the dept_CAPSTONE file
        '''
        dept_df = pd.read_csv(self.path+"/dept_CAPSTONE.txt", names=["dept_code", "dept_name"], index_col=False)
        dept_df.fillna("", inplace=True)
        dept_df.drop([0], inplace=True)
        for col in dept_df.columns:
            dept_df[col] = dept_df[col].apply(lambda x: "".join([i if ord(i.lower())in range(97, 123) or i == "." or i == " " else "" for i in x]))
            if col == "dept_code":
                dept_df[col] = dept_df[col].apply(lambda x: x.upper())
            else:
                dept_df[col] = dept_df[col].apply(lambda x: x.title())
        return dept_df
    
    def emp_list_with_dept(self):
        '''
        Merges df with all employees and corresponding dept_code & dept_name
        '''
        emp_list = self.emp_list_df()
        dept_file = self.dept_file_df()
        return emp_list.merge(dept_file, left_on=emp_list.dept, right_on=dept_file.dept_code)

    def emp_active_df(self):
        '''
        Create a list of All Active Employees
        '''
        emp_list = self.emp_list_df()
        [emp_list.drop([n], inplace=True) for n in emp_list.index if emp_list.loc[n, "term_date"] != " "]
        return emp_list

    
    def dept_dict(self):
        '''
        Create a dictionary of dept code and name pairs
        '''
        res = {}
        depts = self.dept_file_df()
        for dpt in depts.dept_code.unique():
            res[dpt] = depts[depts.dept_code == dpt].dept_name.to_list()[0]
        return res
    
    def emp_status_df(self, data):
        '''
        Adds appropriate value to the column 'status
        '''
        for num in data.index:
            if int(data.loc[num, 's_g']) in range(5,8):
                data.loc[num, 'status'] = 'EXECUTIVE'
            else:
                data.loc[num, 'status'] = 'NON-EXECUTIVE'
        return data

    def emp_id_df(self):
        '''
        Generates employee id (i.e., Last_name[:3]+First_name[:3]+3_digits)
        Create new column 'emp_id' and fills it with the unique id
        '''
        df = self.emp_active_df()
        all_digits = random.sample(["00"+str(n) for n in range(1,10)]+[str(num) if num > 99 else "0"+str(num) for num in range(10,1000)], 617)
        df['emp_id'] = ""
        for n in range(len(all_digits)):
            name = df.iloc[n, 0].replace(" ", "").split(",")
            df.iloc[n, 7] = name[0][:3]+name[1][:3].upper()+all_digits[n]
        df.sort_values(by=["emp_id"], ascending=True, inplace=True)
        return df

    def raise_schedule_df(self):
        '''
        Loads and creates a df from the file containing raise schedule
        '''
        df = pd.read_csv(self.path+"/raises_CAPSTONE.txt")
        df.raise_amount = df.raise_amount.apply(lambda x: x.strip("'"))
        df["ratio"] = round(df.raise_amount.apply(lambda x: float(x.strip("%")))/100, 4)
        df.loc[6, "years"], df.loc[6,"ratio"] = 0, 0
        return df

    def raise_years(self, var_x):
        '''
        Takes an employees tenure and converts it to a numeric value that is consistent with the raise schedule
        '''
        if var_x > 10 or var_x in range(7,10):
            var_x = 10
        elif var_x in range(5,7):
            var_x = 5
        elif var_x == 4:
            var_x = 3
        return var_x

    def calc_raise_df(self):
        '''
        Calculates the raise amount and salary after raise. Simultaneously creates new columns 'raise_amount' and 'new_salsry'.
        Fills them with the calculated values
        '''
        df = self.emp_id_df()
        raise_df = self.raise_schedule_df()
        for n in df.index:
            today = pd.Timestamp.today().date()
            years = self.raise_years(round((today - df.loc[n, "hire_date"]).days/365))
            df.loc[n, "raise_amount"] = round(float(df.loc[n, "salary"])*float(raise_df[raise_df.years == years].ratio),2)
            df.loc[n, "new_salary"] = float(df.loc[n, "salary"])+df.loc[n, "raise_amount"]
        return df

    def salary_range_df(self):
        '''
        Reads and cleans the file containing salary schedule. Then creates a df with starting and ending salary amonut for each salary grade.
        '''
        df = pd.read_csv(self.path+"/salary_grade_CAPSTONE.txt", names=["sg", "start", "end"], index_col=False)
        df.drop([0], inplace=True)
        df.fillna("", inplace=True)
        for col in ["start", "end"]:
            df[col] = df[col].apply(lambda x: "".join([i for i in x if ord(i) in range(48,58)]))
        return df

    def histgram(self, data, **x):
        '''
        Creates histgram, saves it as an image and returns the url
        '''
        ax, fig, col, key, target, bin = x["ax"], x["fig"], x["col"], x["key"], x["target"], x["bin"]
        ax.hist(height=data[data[col] == key][target].count(), x=key, bins=bin)
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        image = base64.b64encode(buf.getbuffer()).decode("ascii")
        url = f"img src=data:image/png;base64,{image}"
        return url

    def unique_values(self, data, target):
        '''
        Generates a list of unique values in a column
        '''
        return data.sort_values(by=[target])[target].unique()
    
    def chart_pie1(self, data_dict):
        '''
        Takes a dict of parameters to generate a pie chart.
        Creates an image and return url
        '''
        fig = Figure(figsize=(8, 5))
        ax = fig.add_subplot(1,1,1)
        ax.set_title(label=data_dict['title'], fontdict={'color':"black", 'fontsize':20})
        ax.pie(data_dict['data'].values(), explode=data_dict['explode'], autopct="%1.2f%%", labels=data_dict['labels'], textprops={'color':"black", 'size':13}, colors=(i for i in data_dict['colors']))
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        image = base64.b64encode(buf.getbuffer()).decode("ascii")
        url = f"img src=data:image/png;base64,{image}"
        return url
    
    def scatter_plot(self, var_x, var_y, title, colors):
        '''
        Generates a scatter plot, creates an image, and returns the url
        '''
        fig = Figure(figsize=(8, 5))
        ax = fig.add_subplot(1,1,1)
        ax.set_title(label=title, fontdict={'color':"black", 'fontsize':20})
        ax.scatter(var_x, var_y, c=colors)
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        image = base64.b64encode(buf.getbuffer()).decode("ascii")
        url = f"img src=data:image/png;base64,{image}"
        return url
    
    def chart_barh(self, data):
        '''
        Takes a dict of parameters to generate a horizontal bar chart.
        Creates an image and return url
        '''
        fig = Figure(figsize=(8, 5))
        ax = fig.add_subplot(1,1,1)
        ax.set_ylim(top=data['ylim']) if data['ylim'] else None
        ax.set_title(label=data['title'], fontdict={'color':"black", 'fontsize':20})
        ax.barh(width=data['width'], y=data['ylabel'], height=data['height'], color=data['color'])
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        image = base64.b64encode(buf.getbuffer()).decode("ascii")
        url = f"img src=data:image/png;base64,{image}"
        return url

    def chart_bar(self, data):
        '''
        Takes a dict of parameters to generate a bar chart.
        Creates an image and return url
        '''
        fig = Figure(figsize=(8, 5))
        ax = fig.add_subplot(1,1,1)
        ax.set_title(label=data['title'], fontdict={'color':"black", 'fontsize':20})
        ax.bar(x=data['xlabel'], height=data['data'].values(), color=['orangered', 'lightcoral', 'gold', 'lightseagreen',  'cornflowerblue', 'mediumpurple', 'slategray'])
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        image = base64.b64encode(buf.getbuffer()).decode("ascii")
        url = f"img src=data:image/png;base64,{image}"
        return url
    
    def chart_distribution(self, data, title, chart_color):
        '''
        Generates a distribution chart.
        Creates an image and return url
        '''
        data = pd.Series(data)
        data = data.apply(lambda x: float(x))
        stats = self.basic_stats(data)
        fig_plot = Figure(figsize=(13, 7))
        ax_plot = fig_plot.add_subplot(1,1,1)
        ax_plot.set_title(label=title, fontdict={'color':"black", 'fontsize':20})
        cnt, bin_n, patches = ax_plot.hist(data, 80, color=chart_color)
        ax_plot.plot(bin_n[:-1], cnt, '--', color='dimgray')
        stats = f"Mean: {stats['mean']}\nMedian: {stats['median']}\nMode(s): {stats['modes']}\nSD: {stats['sd']}"
        ax_plot.text(0.85, 0.73, stats, ha='right', va='top', transform=fig_plot.transFigure,fontdict={"fontsize": 13})
        buf = io.BytesIO()
        fig_plot.savefig(buf, format="png")
        image = base64.b64encode(buf.getbuffer()).decode("ascii")
        url = f"img src=data:image/png;base64,{image}"
        return url
    
    def chart_pie(self, *args, **kwargs):
        '''
        Takes data as args and other parameters as kwargs and creates a pie chart and url for the image
        '''
        fig_pie = Figure(figsize=(8, 5))
        ax_pie = fig_pie.add_subplot(1,1,1)
        ax_pie.set_title(label=kwargs['title'], fontdict={'color':"black", 'fontsize':20})
        ax_pie.pie(args, explode=kwargs['explode'], autopct="%1.2f%%", labels=kwargs['labels'], textprops={'color':"black", 'size':17}, colors=(i for i in kwargs['colors']))
        buf = io.BytesIO()
        fig_pie.savefig(buf, format="png")
        image = base64.b64encode(buf.getbuffer()).decode("ascii")
        url = f"img src=data:image/png;base64,{image}"
        return url


    def find_modes(self, var_x):
        '''
        finds the mode(s) in a data set amd returns all of them if multiple
        '''
        data, modes = {}, {}
        var_x.sort()
        cnt = 1
        for num in range(1,len(var_x)):
            if var_x[num] == var_x[num-1]:
                cnt += 1
            else:
                data[var_x[num-1]] = cnt
                cnt = 1
        for key,val in data.items():
            if val == max(data.values()) and val > 1:
                modes[key] = val
        return modes if len(modes.values()) > 0 else "No Modes"
    
    def basic_stats(self, data_list):
        '''
        Calculates and returns basic stats values as a dict
        '''
        stats = {}
        data = pd.Series(data_list)
        data = data.apply(lambda x: float(x))
        stats["sample_size"], stats["mean"], stats["median"], stats["modes"], stats["sd"] = data.count(), data.mean().round(4), data.median().round(4), self.find_modes(data.to_numpy().astype(np.float64)), data.std().round(4)
        return stats
    
    def project_description(self, file):
        '''
        Reads a file passed in line by line as string values and appends to a list
        Used to print project description on the project's home screen
        '''
        texts = []
        with open(file, 'r') as file:
            line = file.readline()
            cnt = 0
            while line:
                if len(line) > 5:
                    texts.append(line)
                line = file.readline()
                cnt += 1
        return texts

if __name__ == "__main__":
    EmpAnalysis.__init__()
                





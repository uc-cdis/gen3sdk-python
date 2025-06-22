from IPython.display import display, HTML
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pandas.tools.plotting import table

import gen3
from gen3.submission import Gen3Submission

import json
import requests
import os

class Gen3Error(Exception):
    pass


class Gen3Analysis(Gen3Submission):
    """Analysis functions for exploratory data analysis in a Gen3 Data Commons.

    A class for interacting with the Gen3 query and data export services.
    Supports generating tables and plots to explore data in a Gen3 Data Commons.

    Args:
        # endpoint (str): The URL of the data commons.
        # auth_provider (Gen3Auth): A Gen3Auth class instance.

    Examples:
        This generates the Gen3Analysis class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> endpoint = "https://nci-crdc-demo.datacommons.io"
        ... auth = Gen3Auth(endpoint, refresh_file="credentials.json")
        ... sub = Gen3Submission(endpoint, auth)
        ... analysis = Gen3Analysis(sub, auth)

    """

    def __init__(self, endpoint, auth_provider):
        self._auth_provider = auth_provider
        self._endpoint = endpoint

    def __export_file(self, filename, output):
        """Writes an API response to a file.
        """
        outfile = open(filename, "w")
        outfile.write(output)
        outfile.close
        print("\nOutput written to file: "+filename)

    def plot_categorical_property(self, property,df):
        #plot a bar graph of categorical variable counts in a dataframe
        df = df[df[property].notnull()]
        N = len(df)
        categories, counts = zip(*Counter(df[property]).items())
        y_pos = np.arange(len(categories))
        plt.bar(y_pos, counts, align='center', alpha=0.5)
        #plt.figtext(.8, .8, 'N = '+str(N))
        plt.xticks(y_pos, categories)
        plt.ylabel('Counts')
        plt.title(str('Counts by '+property+' (N = '+str(N)+')'))
        plt.xticks(rotation=90, horizontalalignment='center')
        #add N for each bar
        plt.show()

    def plot_numeric_property(self, property,df,by_project=False):
        #plot a histogram of numeric variable in a dataframe
        df = df[df[property].notnull()]
        data = list(df[property])
        N = len(data)
        fig = sns.distplot(data, hist=False, kde=True,
                 bins=int(180/5), color = 'darkblue',
                 kde_kws={'linewidth': 2})
        plt.figtext(.8, .8, 'N = '+str(N))
        plt.xlabel(property)
        plt.ylabel("Probability")
        plt.title("PDF for all projects "+property+' (N = '+str(N)+')') # You can comment this line out if you don't need title
        plt.show(fig)

        if by_project is True:
            projects = list(set(df['project_id']))
            for project in projects:
                proj_df = df[df['project_id']==project]
                data = list(proj_df[property])
                N = len(data)
                fig = sns.distplot(data, hist=False, kde=True,
                         bins=int(180/5), color = 'darkblue',
                         kde_kws={'linewidth': 2})
                plt.figtext(.8, .8, 'N = '+str(N))
                plt.xlabel(property)
                plt.ylabel("Probability")
                plt.title("PDF for "+property+' in ' + project+' (N = '+str(N)+')') # You can comment this line out if you don't need title
                plt.show(fig)

    def node_record_counts(self, project_id):
        query_txt = """{node (first:-1, project_id:"%s"){type}}""" % (project_id)
        res = Gen3Submission.query(query_txt)
        df = json_normalize(res['data']['node'])
        counts = Counter(df['type'])
        df = pd.DataFrame.from_dict(counts, orient='index').reset_index()
        df = df.rename(columns={'index':'node', 0:'count'})
        return df

    def property_counts_table(self, prop, df):
        df = df[df[prop].notnull()]
        counts = Counter(df[prop])
        if len(counts) > 0:
            df1 = pd.DataFrame.from_dict(counts, orient='index').reset_index()
            df1 = df1.rename(columns={'index':prop, 0:'count'}).sort_values(by='count', ascending=False)
            with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                display(df1)
        else:
            print("Length of DataFrame is zero.")

    def property_counts_by_project(self, prop, df):
        df = df[df[prop].notnull()]
        categories = list(set(df[prop]))
        projects = list(set(df['project_id']))

        project_table = pd.DataFrame(columns=['Project','Total']+categories)
        project_table

        proj_counts = {}
        for project in projects:
            cat_counts = {}
            cat_counts['Project'] = project
            df1 = df.loc[df['project_id']==project]
            total = 0
            for category in categories:
                cat_count = len(df1.loc[df1[prop]==category])
                total+=cat_count
                cat_counts[category] = cat_count

            cat_counts['Total'] = total
            index = len(project_table)
            for key in list(cat_counts.keys()):
                project_table.loc[index,key] = cat_counts[key]

            project_table = project_table.sort_values(by='Total', ascending=False, na_position='first')

        display(project_table)
        return project_table

    def save_table_image(self, df,filename='mytable.png'):
        """ Saves a pandas DataFrame as a PNG image file.
        """
        ax = plt.subplot(111, frame_on=False) # no visible frame
        ax.xaxis.set_visible(False)  # hide the x axis
        ax.yaxis.set_visible(False)  # hide the y axis
        table(ax, df)  # where df is your data frame
        plt.savefig(filename)

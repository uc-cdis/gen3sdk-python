import os
import re
import sys
import math
import glob
import argparse
import pandas as pd
from datetime import datetime
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import FoldedScalarString as fss
from ruamel.yaml.scalarstring import DoubleQuotedScalarString as dbl_quote
from ruamel.yaml.comments import CommentedMap as cmap
from math import isnan


class Gen3Error(Exception):
    pass

class Gen3Datamodel:
    """Advanced scripts for creating and manipulating a Gen3 Data Dcitionary
    """

    # def __init__(self, endpoint, auth_provider):
    #     self._auth_provider = auth_provider
    #     self._endpoint = endpoint
    #     self.sub = Gen3Submission(endpoint, auth_provider)
    #     self.exp = Gen3Expansion(endpoint, auth_provider)


    def stripper(self, string):

        if isinstance(string, str):
            string = string.strip()
            string = string.strip('|"\',')
        elif isinstance(string, float):
            if isnan(string):
                return None
        return string


    # def get_params():
    #     """
    #     Parse the arguments passed to program
    #     """
    #
    #     parser = argparse.ArgumentParser(description='Specify whether yamls are generated with terms and enumDefs, name of directory containing target nodes, uses enum and nodeterms, and variables TSV files, and name of output dictionary.')
    #
    #     # parser.add_argument('-t', '--terms', dest='terms_flag', required=False, help='Use "-terms et" to generate yamls with original specification and "-terms at" to generate yamls with no terms or enumDefs')
    #     parser.add_argument('-i', '--in_dir', dest='in_dir', required=True, help='Location of the nodes tsv')
    #     parser.add_argument('-o', '--out_dir', dest='out_dir', required=True, help='Location of the output yamls')
    #     parser.add_argument('-e', '--extension', dest='extension', default='xlsx', choices= ['tsv', 'txt', 'xlsx'], help='Extension of the files to be read')
    #     parser.add_argument('-t', '--terms_file', dest='terms_file', action='store_true', help='flag to generate terms file YAML')
    #
    #     args = parser.parse_args()
    #
    #     return args

    def my_represent_none(self, data):
        return self.represent_scalar(u'tag:yaml.org,2002:null', u'null')


    def my_represent_none_blank(self, data):
        return self.represent_scalar(u'tag:yaml.org,2002:null', u'')


    def validate_enum(self, temp_enum):
        """
        Strips spaces & converts values that could be interpreted in yaml as
        nonstring, to double quotation string
        """

        enum = stripper(temp_enum)
        # enum = enum.replace(':', '-')

        if enum != 'open' and '/' not in enum:
            if isinstance(enum, str) and enum.lower() in ['yes', 'no', 'true', 'false', 'root', 'na']:
                enum = dbl_quote(enum)

            else:
                try:
                    enum_ = eval(enum) # convert to int

                    if enum_ == 0:
                        enum = dbl_quote(enum)

                    elif (isinstance(enum_, int) or isinstance(enum_, float)) and not(re.search('''[^0-9]''',enum)) :
                        enum = dbl_quote(enum)

                except:
                    try:
                        enum_ = int(enum)
                        enum  = dbl_quote(enum)

                    except:
                        try:
                            enum_ = float(enum)
                            enum  = dbl_quote(enum)

                        except:
                            pass

        return enum


    def validate_name(self, string, name_type):
        """
        Validates the node & property names
        """

        if type(string) != str and math.isnan(string):
            return None

        match = None

        if name_type == 'node':
            match = re.search('''[^a-zA-Z_]''', string)

        elif name_type == 'property':
            match = re.search('''[^a-zA-Z_0-9]''', string)

        if match:
            sys.exit('Illegal character {0} found in node/property name {1}. Only lowercase letters and underscore allowed.'.format(match, string))

        return string.lower()


    def get_terms(self, terms):
        """
        Converts terms string into list of terms
        """

        if isinstance(terms, str):
            lterms = terms.split(',')
            terms  = [{'$ref': dbl_quote(stripper(t))} for t in lterms]

            return terms

        return None


    def validate_desc(self, string):
        """
        Validate the description text
        """

        if string is None:
            return None

        string = ' '.join([stripper(s) for s in string.split('\n')])
        string = stripper(string)

        return string


    def reqs2list(self, string):
        """
        Converts comma separated value string into list
        """

        if isinstance(string, type(None)):
            return []

        elif isinstance(string, str):
            rlis = [stripper(r) for r in string.split(',')]

            return rlis

        return string


    def string2list(self, key, val):
        """
        Converts complex comma & pipe separated link string into list of lists
        """

        link_lst = []

        # in some cases val is False which will be evaluated & will return empty list
        if val is not None:
            val = str(val)

            for i in val.split('|'):
                tmp_lst= []

                for j in i.split(','):
                    j_ = stripper(j)

                    if key in ['required', 'group_required', 'group_exclusive']:
                        if j_ is not None and j_ != '':
                            tmp_lst.append(eval(j_.title()))
                        else:
                            tmp_lst.append('')

                    else:
                        tmp_lst.append(j_)

                link_lst.append(tmp_lst)

            return link_lst

        return [['']]


    def property_reference_setter(self, multiplicity):
        """
        Creates a reference for each link based on multiplicity to populate in the
        properties block
        """

        if multiplicity in ['many_to_one', 'one_to_one']:
            return {'$ref':dbl_quote('_definitions.yaml#/to_one') }

        else:
            return {'$ref':dbl_quote('_definitions.yaml#/to_many') }


    def build_enums(self, enum_df):
        """
        Converts enum dataframe into proper dict of dict containing enums & enumDefs
        """

        enum_list = enum_df.to_dict('records')
        enum_dict = {}

        for enum in enum_list:
            node     = ''
            field    = ''
            enum_val = ''
            enum_def = ''
            dep_enum = ''

            for key, val in enum.items():
                if val:
                    key = key[1:-1]

                    if key == 'node':
                        node = validate_name(val, 'node')

                    elif key == 'property':
                        field = validate_name(val, 'property')

                    elif key == 'enum_value':
                        enum_val = validate_enum(val)

                    elif key == 'enum_def':
                        enum_def = val

                    elif key == 'deprecated':
                        dep_enum = val

            if node != '':
                if node not in enum_dict:
                    enum_dict[node] = {}

                if field != '':
                    if field not in enum_dict[node]:
                        enum_dict[node][field] = {}

                    if 'enum' not in enum_dict[node][field]:
                        enum_dict[node][field]['enum']            = []
                        enum_dict[node][field]['deprecated_enum'] = []
                        enum_dict[node][field]['enumDef']         = {}

                    if not dep_enum:
                        enum_dict[node][field]['enum'].append(enum_val)

                    if dep_enum == 'yes':
                        enum_dict[node][field]['deprecated_enum'].append(enum_val)

                    if enum_def == 'common':
                        enum_dict[node][field]['enumDef'][enum_val] = {'$ref': [dbl_quote('_terms.yaml#/'+re.sub('[\W]+', '', enum_val.lower().strip().replace(' ', '_'))+'/'+enum_def)]}

                    elif enum_def == 'specific':
                        enum_dict[node][field]['enumDef'][enum_val] = {'$ref': [dbl_quote('_terms.yaml#/'+re.sub('[\W]+', '', enum_val.lower().strip().replace(' ', '_'))+'/'+node+'/'+field)]}

                    elif enum_def:
                        enum_dict[node][field]['enumDef'][enum_val] = {'$ref': [dbl_quote(stripper(x)) for x in enum_def.split(',')]}

        # Validate deprecated enums present in enum section
        missing_deprecated_enums = []

        for key, val in enum_dict.items():
            for k,v in val.items():
                if 'deprecated_enum' in v:
                    for d in v['deprecated_enum']:
                        if d not in v['enum']:
                            missing_deprecated_enums.append(d + ' - ' + key + ' : ' + k)

        if missing_deprecated_enums !=[]:
            sys.exit('ERROR : Missing enum defs in main section for below deprecated enums: \n{0}'.format(missing_deprecated_enums))

        # Remove empty deprecated_enum, enumDefs
        for key, val in enum_dict.items():
            for k,v in val.items():
                if 'deprecated_enum' in v and v['deprecated_enum'] == []:
                    v.pop('deprecated_enum')

                if 'enumDef' in v and v['enumDef'] == {}:
                    v.pop('enumDef')

        return enum_dict


    def build_properties(self, variables_df, enum_df):
        """
        Converts variables dataframe into proper dict of dict containing variable
        description, type & enums
        """

        var_list  = variables_df.to_dict('records')
        var_dict  = {}

        if enum_df is not None:
            enum_dict = build_enums(enum_df)

        else:
            enum_dict = {}

        for var in var_list:
            temp_var  = {}
            node      = ''
            field     = ''

            for key, val in var.items():
                if val is not None:
                    key = key[1:-1]

                    if key == 'node':
                        node = validate_name(val, 'node')

                    elif key == 'property':
                        field = validate_name(val, 'property')

                    elif key == 'terms':
                        val_ = reqs2list(val.lower())

                        for v in val_:
                            if '$ref' not in temp_var:
                                temp_var['$ref'] = []

                            if v == 'common':
                                temp_var['$ref'].append(dbl_quote('_terms.yaml#/'+field.lower().strip().replace(' ', '_')+'/'+v))

                            elif v == 'specific':
                                temp_var['$ref'].append(dbl_quote('_terms.yaml#/'+field.lower().strip().replace(' ', '_')+'/'+node+'/'+v))

                            elif v:
                                temp_var['$ref'].append(dbl_quote(v))


                            '''
                            # Do not delete - for old format
                            if '_terms.yaml' in v:
                                if 'term' not in temp_var:
                                    temp_var['term'] = {}
                                    temp_var['term']['$ref'] = []

                                temp_var['term']['$ref'].append(dbl_quote(v))

                            else:
                                if '$ref' not in temp_var:
                                    temp_var['$ref'] = []

                                temp_var['$ref'].append(dbl_quote(v))
                            '''

                    elif key == 'description':
                        if val:
                            val = fss(validate_desc(val))

                        temp_var[key] = val

                    elif key == 'pattern':
                        temp_var[key] = dbl_quote(val)

                    elif key == 'default':
                        if isinstance(val,str) and val.title() in ['True', 'False']:
                            val = eval(val.title())

                        temp_var[key] = val

                    elif key == 'type' and val != 'enum':
                        val_ = reqs2list(val)

                        if len(val_) == 1:
                            temp_var[key] = val_[0]

                        else:
                            temp_type = []

                            for v in val_:
                                if v == 'null':
                                    v= dbl_quote(v)

                                temp_type.append({'type' : v})

                            temp_var['oneOf'] = temp_type

                    elif key != 'type':
                        temp_var[key] = val

            if 'oneOf' in temp_var:
                var_keys = ['maximum', 'minimum', 'pattern']

                for k in var_keys:
                    if k in temp_var:
                        for t in temp_var['oneOf']:
                            if t['type'] != 'null' and k != 'pattern':
                                t[k] = int(temp_var.pop(k))

                            elif t['type'] != 'null' and k == 'pattern':
                                t[k] = temp_var.pop(k)

            if 'maximum' in temp_var:
                temp_var['maximum'] = int(temp_var.pop('maximum'))

            if 'minimum' in temp_var:
                temp_var['minimum'] = int(temp_var.pop('minimum'))

            # When type is enum it is not populated in the temp_var as temp_var is constructed
            # to populate the actual values supposed to be populated in yaml
            if 'type' not in temp_var and node in enum_dict and field in enum_dict[node]:
                for k,v in enum_dict[node][field].items():
                    temp_var[k] = v

            if node != '' and field != '':
                if node in var_dict:
                    var_dict[node][field] = temp_var

                else:
                    var_dict[node]        = {}
                    var_dict[node][field] = temp_var

        return var_dict


    def validate_links(self, link_dict, node_name):
        """
        Validates the link structure to ensure same number of subgroups are added
        """

        len_links = {}
        flag      = True

        for key, val in link_dict.items():
            len_links[key]= [len(v) for v in val]

        ref_link = len_links['name']
        link_len = len(ref_link)

        for key, val in len_links.items():
            if len(val)!= link_len:
                flag = False
                print('\n{0} : "{1}" - {2} do not have same number of subgroups as "name" - has {3} subgroups'.format(node_name, key, len(val), link_len))

            if key in ['group_exclusive','group_required']:
                for i in range(len(ref_link)):
                   if (not(ref_link[i]>0 and val[i]==1)):
                        flag = False
                        print('\n{0} : subgroups in "{1}" - {2} do not have atleast 1 or have more than 1 values for each subgroup in "name"  - has {3} in each subgroup'.format(node_name, key, val, ref_link))

            else:
                if val != ref_link:
                    flag = False
                    print('\n{0} : subgroups in "{1}" - {2} do not have same number of values as "name" subgroup - has {3} in each subgroup'.format(node_name, key, val, ref_link))

        return flag


    def add_links(self, link_dict, node_name):
        """
        Builds a links dictionary template and adds values from the input data then
        merges to the main node dictionary
        """

        links     = []
        link_refs = {}

        if type(link_dict['name']) != list or link_dict['name'] == [['']]:
            return links, link_refs

        valid_links = validate_links(link_dict, node_name)

        if valid_links:
            for i in range(len(link_dict['name'])):
                if link_dict['group_required'][i] != [''] and len(link_dict['name'][i]) > 1:
                    subgroups = []

                    for l in range(len(link_dict['name'][i])):
                        subgroup = {'name'        : link_dict['name'][i][l],
                                    'backref'     : link_dict['backref'][i][l],
                                    'label'       : link_dict['label'][i][l],
                                    'target_type' : link_dict['target'][i][l],
                                    'multiplicity': link_dict['multiplicity'][i][l],
                                    'required'    : link_dict['required'][i][l]
                                   }

                        subgroups.append(subgroup)
                        link_refs[link_dict['name'][i][l]] = property_reference_setter(link_dict['multiplicity'][i][l])

                    sub = {'exclusive': link_dict['group_exclusive'][i][0], 'required': link_dict['group_required'][i][0], 'subgroup': subgroups}
                    links.append(sub)

                elif link_dict['group_required'][i] == [''] and len(link_dict['name'][i]) == 1:
                    for l in range(len(link_dict['name'][i])):
                        link = {'name'        : link_dict['name'][i][l],
                                'backref'     : link_dict['backref'][i][l],
                                'label'       : link_dict['label'][i][l],
                                'target_type' : link_dict['target'][i][l],
                                'multiplicity': link_dict['multiplicity'][i][l],
                                'required'    : link_dict['required'][i][l]
                               }

                        links.append(link)
                        link_refs[link_dict['name'][i][l]] = property_reference_setter(link_dict['multiplicity'][i][l])

        else:
            sys.exit('ERROR: fix the above link issues')

        return links, link_refs


    def build_nodes(self, nodes_df, var_dict): #, terms_flag):
        """
        Builds a python dictionary that will be used as a template for constructing
        node yaml file
        """

        # Transform nodes tsv into a dictionary and process fields
        nodedicts     = nodes_df.to_dict('records')
        dict_of_nodes = {}

        for node in nodedicts:
            out_dict1     = {}
            out_dict2     = {}
            out_dict3     = {}
            out_dict4     = {}
            out_dict5     = {}
            out_dict6     = {}
            link_dict     = {}
            property_ref  = ''

            for key, val in node.items():
                key = key[1:-1]

                if key == '$schema':
                    out_dict1[key] = dbl_quote(val)

                elif key == 'id':
                    out_dict2[key] = dbl_quote(validate_name(val, 'node'))

                elif key == 'description':
                    if val:
                        val = fss(validate_desc(val))

                    out_dict2[key] = val

                elif key == 'systemProperties':
                    out_dict3[key] = reqs2list(val)

                elif key == 'required':
                    val_ = reqs2list(val)

                    if val_:
                        out_dict4[key] = val_

                elif key == 'uniqueKeys':
                    out_dict5[key] = string2list(key, val)

                elif key == 'deprecated':
                    if val:
                        out_dict6[key] = reqs2list(val)

                elif key == 'root':
                    if val:
                        out_dict2[key] = val

                elif key == 'property_ref':
                    property_ref = val

                elif key == 'nodeTerms': # and terms_flag == 'et': Check this flag value if its correct
                    val_ = get_terms(val)

                    if val_:
                        out_dict2[key] = val_

                elif 'link_' in key:
                    key_ = key.replace('link_','')

                    link_dict[key_]= string2list(key_, val)

                else:
                    out_dict2[key] = val

            # Add formatted links to each dictonary
            links, link_refs =  add_links(link_dict, out_dict2['id'])

            # Add formatted properties to each dictonary
            properties = {}

            if property_ref and property_ref != '':
                properties['$ref'] = [dbl_quote(property_ref)]

            if out_dict2['id'] in var_dict:
                for key, val in var_dict[out_dict2['id']].items():
                    properties[key] = val

            for key, val in link_refs.items():
                properties[key] = val

            dict_of_nodes[out_dict2['id']] = [item for item in [out_dict1, out_dict2, out_dict3, {'links' : links}, out_dict4, out_dict5, out_dict6, {'properties' : properties}] if item]

        return dict_of_nodes


    def build_yamls(self, nodes_in_file, var_in_file, enum_in_file, in_dir, out_dir, extension): #terms_flag,
        """
        Constructs node yaml file
        """

        if extension == 'xlsx':
            xlsx_file  = pd.ExcelFile(nodes_in_file)
            node_sheet = None
            var_sheet  = None
            enum_sheet = None

            for i in xlsx_file.sheet_names:
                if 'nodes_' in i:
                    node_sheet = i
                if 'variables_' in i:
                    var_sheet  = i
                if 'enums_' in i:
                    enum_sheet = i

            if not(node_sheet) or not(var_sheet) or not(enum_sheet):
                sys.exit('ERROR: one or more than one of the sheets (Nodes, Variable & Enum) not found, exiting the program')

            nodes_df     = xlsx_file.parse(sheet_name = node_sheet, index_col=None, header=0, keep_default_na=False, na_values=[''])
            variables_df = xlsx_file.parse(sheet_name = var_sheet, index_col=None, header=0, keep_default_na=False, na_values=[''])
            enum_df      = xlsx_file.parse(sheet_name = enum_sheet, index_col=None, header=0, keep_default_na=False, na_values=[''])

            # nodes_df     = nodes_df.where(nodes_df.notnull(), None)
            # variables_df = variables_df.where(variables_df.notnull(), None)
            # enum_df      = enum_df.where(enum_df.notnull(), None)

        else:
            nodes_df     = pd.read_csv(nodes_in_file, index_col=None, header=0, sep = '\t', keep_default_na=False, na_values=[''])
            variables_df = pd.read_csv(var_in_file, index_col=None, header=0, sep = '\t', keep_default_na=False, na_values=[''])

            # nodes_df     = nodes_df.where(nodes_df.notnull(), None)
            # variables_df = variables_df.where(variables_df.notnull(), None)

            try:
                enum_df  = pd.read_csv(enum_in_file, index_col=None, header=0, sep = '\t', keep_default_na=False, na_values=[''])
                # enum_df  = enum_df.where(enum_df.notnull(), None)
            except pd.io.common.EmptyDataError:
                enum_df  = None

        nodes_df     = nodes_df.where(nodes_df.notnull(), None)
        variables_df = variables_df.where(variables_df.notnull(), None)

        if enum_df is not None:
            enum_df  = enum_df.where(enum_df.notnull(), None)

        var_dict     = build_properties(variables_df, enum_df)
        node_dict    = build_nodes(nodes_df, var_dict) #, terms_flag)

        num_nodes    = len(node_dict.keys())
        num_props    = 0

        yaml = YAML()
        yaml.default_flow_style = False
        yaml.indent(offset = 2, sequence = 4, mapping = 2)
        yaml.representer.add_representer(type(None), my_represent_none)

        for key, val in node_dict.items():
            with open('{0}{1}.yaml'.format(out_dir, key), 'w') as file:
                for block in val:
                    if 'properties' in block:
                        num_props += len(block['properties'].keys())
                        dataprop   = cmap(block['properties'])

                        # insert blank lines in properties
                        for k in block['properties'].keys():
                            dataprop.yaml_set_comment_before_after_key(k, before='\n')

                        yaml.dump({'properties': dataprop}, file)

                    elif 'uniqueKeys' in block:
                        block = cmap(block)

                        yaml1 = YAML()
                        yaml1.default_flow_style = None
                        yaml1.indent(offset = 2, sequence = 4, mapping = 2)
                        yaml1.representer.add_representer(type(None), my_represent_none)

                        yaml1.dump(block, file)
                        file.write('\n')

                    else:
                        yaml.dump(block, file)
                        file.write('\n')

        print('*'*100, '\n')
        print(' '*42, 'TSV  ---->  YAML', ' '*42, '\n')
        print('*'*100, '\n')
        print('Source Directory      : {0}'.format(in_dir), '\n')
        print('Number of Nodes       : {0}'.format(num_nodes))
        print('Number of Properties  : {0}'.format(num_props), '\n')
        print('Destination Directory : {0}'.format(out_dir))
        print('*'*100, '\n')


    def build_terms(self,terms_in_file, in_dir, out_dir, extension):
        """
        Constructs _terms yaml file
        """

        if extension == 'xlsx':
            xlsx_file  = pd.ExcelFile(terms_in_file)
            term_sheet = None

            for i in xlsx_file.sheet_names:
                if 'terms_' in i:
                    term_sheet = i

            if not(term_sheet):
                sys.exit('ERROR: Terms sheet not found, exiting the program')

            terms_df   = xlsx_file.parse(sheet_name = term_sheet, index_col=None, header=0, keep_default_na=False, na_values=[''])

        else:
            terms_df   = pd.read_csv(terms_in_file, index_col=None, header=0, sep = '\t', keep_default_na=False, na_values=[''])


        terms_df       = terms_df.where(terms_df.notnull(), None)

        term_dicts     = terms_df.to_dict('records')

        dict_of_terms  = {'id' : '_terms'}

        for term in term_dicts:
            out_dict     = {}
            property_nm  = ''
            termdef      = {}

            for key, val in term.items():
                key = key[1:-1]

                if key == 'property_or_enum':
                    if val == 'id':
                        property_nm = '_id'

                    else:
                        val_ = re.sub('[\W]+', '', val.lower().strip().replace(' ', '_'))
                        property_nm = validate_enum(val_) # val

                elif key == 'node':
                    node = val

                elif key == 'enum_property':
                    enum = val

                elif key == 'description':
                    if val:
                        val = fss(validate_desc(val))

                    out_dict[key] = val

                elif 'termDef:' in key:
                    key_ = key.replace('termDef:','')

                    if key_ == 'term':
                        if val:
                            val = fss(validate_desc(val))

                        termdef[key_] = val

                    elif key_ == 'term_url':
                        if val:
                            val = dbl_quote(val)

                        termdef[key_] = val

                    elif key_ == 'cde_id':
                        try:
                            termdef[key_] = int(val)

                        except:
                            termdef[key_] = val

                    elif key_ in ['term_id' , 'term_version']:
                        if val:
                            termdef[key_] = val

                    else:
                        termdef[key_] = val

            out_dict['termDef'] = termdef

            if property_nm not in dict_of_terms:
                dict_of_terms[property_nm] = {}

            if node == 'common':
                dict_of_terms[property_nm][node] = out_dict

            else:
                if node in dict_of_terms[property_nm]:
                    dict_of_terms[property_nm][node][enum] = out_dict

                else:
                    dict_of_terms[property_nm][node]       = {}
                    dict_of_terms[property_nm][node][enum] = out_dict

        yaml = YAML()
        yaml.default_flow_style = False
        yaml.indent(offset = 2, sequence = 4, mapping = 2)
        yaml.representer.add_representer(type(None), my_represent_none_blank)

        num_terms  = len(dict_of_terms.keys())
        term_props = cmap(dict_of_terms)

        # insert blank lines in properties
        for k in dict_of_terms.keys():
            term_props.yaml_set_comment_before_after_key(k, before='\n')

        with open('{0}{1}.yaml'.format(out_dir, '_terms'), 'w') as file:
            yaml.dump(term_props, file)

        print('*'*100, '\n')
        print(' '*42, 'TSV  ---->  YAML', ' '*42, '\n')
        print('*'*100, '\n')
        print('Source Directory      : {0}'.format(in_dir), '\n')
        print('Number of Terms       : {0}'.format(num_terms), '\n')
        print('Destination Directory : {0}'.format(out_dir))
        print('*'*100, '\n')


    # if __name__ == '__main__':
    #
    #     temp_st_time = datetime.now()
    #     args         = get_params()
    #
    #     # terms_flag   = args.terms_flag
    #     in_dir       = args.in_dir
    #     out_dir      = args.out_dir
    #     extension    = args.extension
    #     terms_file   = args.terms_file
    #
    #
    #     if in_dir[-1] != '/':
    #         in_dir += '/'
    #
    #     if out_dir[-1] != '/':
    #         out_dir += '/'
    #
    #     if not os.path.exists(out_dir):
    #         os.mkdir(out_dir)
    #
    #
    #     tsvfiles      = glob.glob(in_dir+'*.{0}'.format(extension))
    #
    #     nodes_in_file = None
    #     var_in_file   = None
    #     enum_in_file  = None
    #     terms_in_file = None
    #     nodes_schema  = None
    #     terms_schema  = None
    #
    #     for t in tsvfiles:
    #         fn = t.split('/')[-1]
    #
    #         if extension == 'xlsx':
    #             if 'nodes_schema_' in fn[:13]:
    #                 nodes_schema = t
    #
    #             elif 'terms_schema_' in fn[:13]:
    #                 terms_schema = t
    #
    #         else:
    #             if 'nodes_' in fn[:6]:
    #                 nodes_in_file = t
    #
    #             elif 'variables_' in fn[:10]:
    #                 var_in_file = t
    #
    #             elif 'enums_' in fn[:6]:
    #                 enum_in_file = t
    #
    #             elif 'terms_' in fn[:6]:
    #                 terms_in_file = t
    #
    #     if extension == 'xlsx':
    #         if terms_file and not(terms_schema):
    #             sys.exit('ERROR: Terms file not found, exiting the program')
    #
    #         if not(terms_file) and not(nodes_schema):
    #             sys.exit('ERROR: one or more than one of the files (Nodes, Variable & Enum) not found, exiting the program')
    #
    #         if terms_file:
    #             build_terms(terms_schema, in_dir, out_dir, extension)
    #
    #         else:
    #             build_yamls(nodes_schema, var_in_file, enum_in_file, in_dir, out_dir, extension) # terms_flag
    #
    #     else:
    #         if terms_file and not(terms_in_file):
    #             sys.exit('ERROR: Terms file not found, exiting the program')
    #
    #         if not(terms_file) and (not(nodes_in_file) or not(var_in_file) or not(enum_in_file)):
    #             sys.exit('ERROR: one or more than one of the files (Nodes, Variable & Enum) not found, exiting the program')
    #
    #         if terms_file:
    #             build_terms(terms_in_file, in_dir, out_dir, extension)
    #
    #         else:
    #             build_yamls(nodes_in_file, var_in_file, enum_in_file, in_dir, out_dir, extension) # terms_flag
    #
    #     temp_fin_time = datetime.now()
    #
    #     print('\n\tTotal time for TSV <> YAML generation   : ' + str(temp_fin_time - temp_st_time))

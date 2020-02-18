    def create_project_from_tsvs(self,project_id,tsv_dir,extension='tsv',create_project=True):

        # get the node submission order based on the data dictionary
        submission_order = self.get_submission_order(root_node='project',excluded_schemas=['_definitions','_settings','_terms','program','project','root','data_release','metaschema'])

        # glob files in tsv_dir to get file and node names
        pattern = "{}*{}".format(project_id,extension)
        filenames = glob.glob(pattern)

        nodes = []
        for filename in filenames:
            regex = "{}_(.+).{}".format(project_id,extension)
            match = re.search(regex, filename)
            if match:
                node = match.group(1)
                if node in list(dd):
                    nodes.append(node)
                else:
                    print("The node '{}' is not in the data dictionary! Skipping...".format(node))

        # create project if it doesn't already exist
        if create_project is True:
            prog,proj = project_id.split('-')
            project_txt = """{"type": "project",
                "code": "%s",
                "dbgap_accession_number": "%s",
                "name": "%s"} """ % (proj,proj,proj)
            project_json = json.loads(project_txt)
            try:
                self.sub.create_project(prog,project_json)
            except:
                raise Gen3Error("Couldn't create project!")

        # submit node TSVs in the proper order:

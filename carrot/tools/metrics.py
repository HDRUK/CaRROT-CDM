class Metrics():
    def __init__(self, dataset_name, log_threshold=0):
        self.datasummary={}
        self.allcounts={}
        self.log_data=""
        self.dataset_name=dataset_name
        self.log_threshold = log_threshold

    def get_new_mapstream_counts(self):
        counts = {}
        counts["input_count"] = 0
        counts["invalid_persids"] = 0
        counts["invalid_dates"] = 0
        counts["invalid_source_fields"] = 0
        counts["output_count"] = 0

        return counts

    def add_data(self, desttablename, increment):
        """
        add_data(self, destination table, data increment)
        Apply the contents of a data increment to the stored self.datasummary
        """
        name = increment["name"]
        for datakey, dataitem in increment.items():
            if datakey == "valid_person_id":
                dkey = "NA" + "." + desttablename + "." + name + "." + datakey
                self.add_counts_to_summary(dkey, dataitem)
            elif datakey == "person_id":
                dkey = "NA" + "." + desttablename + "." + name + "." + datakey
                self.add_counts_to_summary(dkey, dataitem)
            elif datakey == "required_fields":
                for fieldname in dataitem:
                    prfx = "NA"
                    if "source_files" in increment:
                        if fieldname in increment["source_files"]:
                            prfx = self.get_prefix(increment["source_files"][fieldname]["table"])
                            dkey = prfx + "." + desttablename + "." + name + "." + fieldname
                            self.add_counts_to_summary(dkey, dataitem[fieldname])

    def get_prefix(self, fname):
        return fname.split(".")[0]

    def add_counts_to_summary(self, dkey, count_block):
        if dkey not in self.datasummary:
            self.datasummary[dkey] = {}
        for counttype in count_block:
            if counttype not in self.datasummary[dkey]:
                self.datasummary[dkey][counttype] = 0
            self.datasummary[dkey][counttype] += int(count_block[counttype])

    def increment_key_count(self, dkey, count_type):
        """
        Intended to work with the mapstream functions
        """
        if dkey not in self.datasummary:
            self.datasummary[dkey] = {}
        if count_type not in self.datasummary[dkey]:
            self.datasummary[dkey][count_type] = 0
        self.datasummary[dkey][count_type] += 1

    def get_summary(self):
        summary_str = "source\ttablename\tname\tcolumn name\tbefore\tafter content check\tpct reject content check\tafter date format check\tpct reject date format\n"

        for dkey in self.datasummary:
            #print(dkey)
            source, tablename, name, colname = dkey.split('.')
            before_count = int(self.datasummary[dkey]["before"])
            after_count = int(self.datasummary[dkey]["after"])
            after_pct = (float)(before_count - after_count) * 100 / before_count
            summary_str += source + "\t" + tablename + "\t" + name + "\t" + colname + "\t" + str(before_count) + "\t" + str(after_count) + "\t" + "{0:.3f}".format(after_pct) + "\t"
            if "after_formatting" in self.datasummary[dkey]:
                after_format_count = int(self.datasummary[dkey]["after_formatting"])
                after_format_pct = (float)(after_count - after_format_count) * 100 / after_count
                summary_str += str(after_format_count) + "\t" + "{0:.3f}".format(after_format_pct) + "\n"
            else:
                summary_str += "NA\tNA\n"

        return summary_str

    def get_data_summary(self):
        return self.datasummary

    def get_mapstream_summary(self):
        summary_str = "dsname\tsource\tsource_field\ttarget\tconcept_id\tadditional\tincount\tinvalid_persid\tinvalid_date\tinvalid_source\toutcount\n"

        for dkey in sorted(self.datasummary):
            try:
                source, fieldname, tablename, concept_id, additional = dkey.split('~')
            except ValueError:
                print("get_mapstream_summary - ValueError: {0}".format(dkey))
                break
              
            source = self.get_prefix(source)
            dvalue = self.datasummary[dkey]

            input_count = "0"
            if "input_count" in dvalue:
                input_count = str(dvalue["input_count"])

            invalid_person_ids = "0"
            if "invalid_person_ids" in dvalue:
                invalid_person_ids = str(dvalue["invalid_person_ids"])

            invalid_source_fields = "0"
            if "invalid_source_fields" in dvalue:
                invalid_source_fields = str(dvalue["invalid_source_fields"])

            invalid_date_fields = "0"
            if "invalid_date_fields" in dvalue:
                invalid_date_fields = str(dvalue["invalid_date_fields"])

            output_count = "0"
            if "output_count" in dvalue:
                output_count = str(dvalue["output_count"])

            if (int(output_count) >= self.log_threshold) or (tablename != "person"):
              summary_str += self.dataset_name + "\t" + source + "\t" + fieldname + "\t" + tablename + "\t" + concept_id + "\t" + additional +"\t" + input_count + "\t" + invalid_person_ids + "\t" + invalid_date_fields + "\t" + invalid_source_fields + "\t" + output_count + "\n"

        return summary_str

    def add_log_data(self, msg):
        self.log_data += msg + "\n"

    def get_log_data(self):
        return self.log_data
    

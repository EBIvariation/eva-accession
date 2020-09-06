from lxml import etree as et
import json
import yaml

#TODO: Move methods to pyutils repository

def get_properties(profile, xml_path):
    tree = et.parse(xml_path)
    root = tree.getroot()

    properties = {}
    for child in root.xpath('//settings/profiles/profile/id[text()="' + profile + '"]/../properties/*'):
        properties[child.tag] = child.text
    return properties


def get_args_from_private_config_file(private_config_file):
    with open(private_config_file) as private_config_file_handle:
        if 'json' in private_config_file:
            return json.load(private_config_file_handle)
        else:
            if 'yml' in private_config_file:
                return yaml.safe_load(private_config_file_handle)
            else:
                raise TypeError('Configuration file should be either json or yaml')
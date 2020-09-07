from lxml import etree as et
import json
import yaml
import urllib.request

#TODO: Move methods to pyutils repository

def get_properties_from_xml_file(profile, xml_path):
    tree = et.parse(xml_path)
    root = tree.getroot()
    return get_profile_properties(profile, root)


def get_properties_from_xml_string(profile, str):
    root = et.fromstring(str)
    return get_profile_properties(profile, root)


def get_profile_properties(profile, root):
    properties = {}
    for property in root.xpath('//settings/profiles/profile/id[text()="' + profile + '"]/../properties/*'):
        properties[property.tag] = property.text
    return properties


def get_eva_settings_xml_string(token):
    url = 'https://api.github.com/repos/EBIvariation/configuration/contents/eva-maven-settings.xml'
    headers = {'Authorization': 'token ' + token, 'Accept' : 'application/vnd.github.raw' }
    request = urllib.request.Request(url, None, headers)
    with urllib.request.urlopen(request) as response:
        return response.read()


def get_args_from_private_config_file(private_config_file):
    with open(private_config_file) as private_config_file_handle:
        if 'json' in private_config_file:
            return json.load(private_config_file_handle)
        else:
            if 'yml' in private_config_file:
                return yaml.safe_load(private_config_file_handle)
            else:
                raise TypeError('Configuration file should be either json or yaml')
# Copyright 2020 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import click
import os
import signal


def close_mongo_port_to_tempmongo(taxonomy_id):
    port_forwarding_process_id_file = "tempmongo_instance_{0}.info".format(taxonomy_id)
    port_forwarding_process_id = int(open(port_forwarding_process_id_file).read().split("\t")[0])
    os.kill(port_forwarding_process_id, signal.SIGTERM)
    print("Killed port forwarding from remote port with signal 1 - SIGTERM!")
    os.remove(port_forwarding_process_id_file)


@click.option("--taxonomy-id", help="ex: 9913", required=True)
@click.command()
def main(taxonomy_id):
    close_mongo_port_to_tempmongo(taxonomy_id)


if __name__ == "__main__":
    main()

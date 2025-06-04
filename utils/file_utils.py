import logging
import os
import pprint
import time
from subprocess import PIPE, Popen

from aviso.settings import BACKUP_CONFIG, ISPROD, sec_context

from utils import GnanaError

logger = logging.getLogger('gnana.%s' % __name__)

def retry(retries=3):
    def retry_decorator(f):
        def wrapper(*args, **kwargs):
            for i in range(retries):
                try:
                    ret = f(*args, **kwargs)
                    break
                except Exception as e:
                    logger.exception("__func__  %s failed! :-( " % (f.__name__))
                    wait_time = (i + 1) * 30
                    logger.info("Waiting for %d seconds before retrying %s" % (wait_time, f.__name__))
                    time.sleep(wait_time)
                    if i == 2:
                        raise e
                    pass
            return ret
        return wrapper
    return retry_decorator


@retry(3)
def process_shell_command(path, process_command_list, command):
        try:
            process_cmd = Popen(process_command_list, cwd=path, stdout=PIPE)
            exit_code = os.waitpid(process_cmd.pid, 0)
            output = process_cmd.communicate()[0]
            if exit_code[1] != 0:
                logger.info("command:%s :error:%s", command, str(output))
                raise GnanaError("command:%s :error:%s" % (command, str(output)))
            return
        except:
            raise


def pull_config_repo():
    try:
        repopath = os.environ.get('CONFIG_REPO', '/opt/gnana/config')
        logger.info("Pulling the repo")
        process_shell_command(repopath, ["git", "pull"], "pull")
        logger.info("repo pulled")
    except:
        logger.warning("Problem in pulling config repository ")
        raise

def gitbackup_dataset(file_name, content, comment):
    status = True
    git_errors = None

    if ISPROD or BACKUP_CONFIG:
        try:
            repopath = os.environ.get('CONFIG_REPO', '/opt/gnana/config')
            pull_config_repo()

            tenantpath = os.path.join(repopath, sec_context.name)
            logger.info(f"tenantpath: {tenantpath}")

            filepath = os.path.join(sec_context.name, file_name)
            logger.info(f"filepath: {filepath}")

            if not os.path.exists(tenantpath):
                os.mkdir(tenantpath)
                logger.info("Created folder with tenant name")

            full_file_path = os.path.join(repopath, filepath)
            with open(full_file_path, "w") as f:
                f.write(pprint.pformat(content, indent=2))
            logger.info("Writing to file completed")

            logger.info("Adding the file")
            process_shell_command(repopath, ["git", "add", filepath], "add")
            logger.info("Added file to the repo")

            process_shell_command(repopath, ["git", "commit", "-m", comment], "commit")
            logger.info("Repo committed")

            logger.info("Pushing the repo")
            process_shell_command(repopath, ["git", "push"], "push")
            logger.info("Config repo updated")
        except Exception as e:
            logger.warning("Problem in updating config repository ")
            git_errors = "Problem in updating config repository %s" % e.message
            logger.info(git_errors)
            status = False

    return (status, git_errors)

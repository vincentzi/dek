FROM glue-base-2.0


ARG INSTALL_ZSH="true"
ARG UPGRADE_PACKAGES="false"

ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=1000

# Install needed packages and setup non-root user. Use a separate RUN statement to add your own dependencies.
COPY library-scripts/*.sh library-scripts/*.env /tmp/library-scripts/
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && /bin/bash /tmp/library-scripts/common-debian.sh "${INSTALL_ZSH}" "${USERNAME}" "${USER_UID}" "${USER_GID}" "${UPGRADE_PACKAGES}" "true" "true" \
    #
    # ****************************************************************************
    # * TODO: Add any additional OS packages you want included in the definition *
    # * here. We want to do this before cleanup to keep the "layer" small.       *
    # ****************************************************************************
    # && apt-get -y install --no-install-recommends <your-package-list-here> \
    #
    && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/* /tmp/library-scripts

# [Optional] Uncomment this section to install additional OS packages.
# RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
#     && apt-get -y install --no-install-recommends <your-package-list-here>


## Install Python packages
COPY ./requirements-dev.txt /tmp/requirements-dev.txt
RUN pip install -r /tmp/requirements-dev.txt

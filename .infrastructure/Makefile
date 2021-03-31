##########################
# Bootstrapping variables
##########################

STACK_NAME?="sm100"
#DEPLOYMENT_BUCKET_NAME?="UNDEFINED"
DEPLOYMENT_BUCKET_PREFIX?=""
SAM_BUILD_EXTRA_ARGS?=""

SC_PRINCIPAL?="role/Admin"
USE_EXISTING_SC_ROLES?="yes"
PROVISION_DEMO_PROJECT?="yes"
STUDIO_USER_NAME?="demouser"

target:
	$(info ${HELP_MESSAGE})
	@exit 0

init: ##=> Install OS deps and dev tools
	$(info [*] Bootstrapping CI system...)
	@$(MAKE) _install_os_packages

build.project: ##=> Build and package the ML Project stack only
	$(info [*] Building ML Project SAM stack...)
	sam build \
			--use-container \
			--template project.sam.yaml \
			$(SAM_BUILD_EXTRA_ARGS) && \
		sam package \
			--s3-bucket $${DEPLOYMENT_BUCKET_NAME} \
			--s3-prefix $${DEPLOYMENT_BUCKET_PREFIX}sam \
			--output-template-file project.tmp.yaml && \
		aws s3 cp project.tmp.yaml \
			s3://$${DEPLOYMENT_BUCKET_NAME}/$${DEPLOYMENT_BUCKET_PREFIX}project.cf.yaml

build.environment: ##=> Build and package the ML Environment stack only
	$(info [*] Building ML Environment SAM stack...)
	export MLProjectCFURL=https://$${DEPLOYMENT_BUCKET_NAME}.s3.`aws configure get region`.amazonaws.com/$${DEPLOYMENT_BUCKET_PREFIX}project.cf.yaml && \
		envsubst '$$MLProjectCFURL' <environment.sam.yaml >environment-postsub.tmp.sam.yaml && \
	  sam build \
			--use-container \
			--template environment-postsub.tmp.sam.yaml \
			$(SAM_BUILD_EXTRA_ARGS) && \
		sam package \
			--s3-bucket $${DEPLOYMENT_BUCKET_NAME} \
			--s3-prefix $${DEPLOYMENT_BUCKET_PREFIX}sam \
			--output-template-file environment.tmp.yaml && \
		aws s3 cp environment.tmp.yaml \
			s3://$${DEPLOYMENT_BUCKET_NAME}/$${DEPLOYMENT_BUCKET_PREFIX}environment.cf.yaml
# (We use envsubst to replace the Project CF template URL in the environment template with the built &
# staged version)

build: ##=> Build SAM template & assets to CloudFormation on S3
	$(info [*] Building all AWS SAM stacks...)
	@$(MAKE) build.project
	@$(MAKE) build.environment

create: ##=> Create environment stack (only)
	$(info [*] Deploying ML Environment...)
	# This option disables rollback for debugging, versus SAM `deploy`
	aws cloudformation create-stack \
			--template-body file://environment.tmp.yaml \
			--stack-name $(STACK_NAME) \
			--capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
			--disable-rollback \
			--parameters \
			  ParameterKey=UseExistingSageMakerServiceCatalogRoles,ParameterValue=$(USE_EXISTING_SC_ROLES) \
				ParameterKey=SCPrincipal,ParameterValue=$(SC_PRINCIPAL) \
				ParameterKey=ProvisionDemoProject,ParameterValue=$(PROVISION_DEMO_PROJECT)

create.project: ##=> Create project stack (only)
	$(info [*] Deploying...)
	# This option disables rollback for debugging, versus SAM `deploy`
	aws cloudformation create-stack \
			--template-body file://project.tmp.yaml \
			--stack-name $(STACK_NAME) \
			--capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND \
			--disable-rollback \
			--parameters ParameterKey=StudioUserName,ParameterValue=$(STUDIO_USER_NAME)

deploy: ##=> Deploy environment (flexible create or update)
	$(info [*] Deploying...)
	sam deploy \
			--template-file environment.tmp.yaml \
			--stack-name $(STACK_NAME) \
			--capabilities CAPABILITY_NAMED_IAM \
			--no-fail-on-empty-changeset \
			--parameter-overrides \
			  	UseExistingSageMakerServiceCatalogRoles=$(USE_EXISTING_SC_ROLES) \
					SCPrincipal=$(SC_PRINCIPAL) \
					ProvisionDemoProject=$(PROVISION_DEMO_PROJECT)

deploy.project: ##=> Deploy project stack (flexible create or update)
	$(info [*] Deploying...)
	sam deploy \
			--template-file project.tmp.yaml \
			--stack-name $(STACK_NAME) \
			--capabilities CAPABILITY_IAM \
			--no-fail-on-empty-changeset \
			--parameter-overrides \
				StudioUserName=$(STUDIO_USER_NAME)

all: ##=> Build and create stack
	@$(MAKE) build
	@$(MAKE) create

delete: ##=> Delete services
	$(info [*] Deleting stack...)
	aws cloudformation delete-stack --stack-name $(STACK_NAME)


#############
#  Helpers  #
#############

_install_os_packages:
	$(info [*] Installing jq...)
	yum install jq -y
	$(info [*] Checking currently installed Python version...)
	python3 --version
	$(info [*] Upgrading Python SAM CLI and CloudFormation linter to the latest version...)
	python3 -m pip install --upgrade --user cfn-lint aws-sam-cli
	npm -g install aws-cdk

define HELP_MESSAGE

	Environment variables:

	STACK_NAME: "sm100"
		Description: CloudFormation stack name to deploy/redeploy to. Remember to reset this parameter
		  if making multiple successive calls e.g. create.project, create.user!
	DEPLOYMENT_BUCKET_NAME: "UNDEFINED"
		Description: Amazon S3 bucket for staging built SAM Lambda bundles and assets
	DEPLOYMENT_BUCKET_PREFIX: ""
		Description: For publishing to a prefix in your deployment bucket, instead of root. Should end
		  in a slash if set.
	SAM_BUILD_EXTRA_ARGS: ""
		Description: Extra arguments to pass to AWS SAM build, if necessary

	SC_PRINCIPAL: "role/Admin"
		Description: (For Environment stack) A valid IAM principal in your account to grant access to
		  the created service catalog portfolio (e.g. the identity you log in to the AWS Console with)
	USE_EXISTING_SC_ROLES: "yes"
		Description: (For Environment stack) Set 'no' to create SageMaker Service Catalog service
		roles in your account, or 'yes' if they exist already.
	PROVISION_DEMO_PROJECT: "yes"
		Description: Set 'yes' to automatically deploy a "demouser" project during environment
		deployment, or 'no' to leave this as a separate manual step (from Service Catalog or CFn)
	STUDIO_USER_NAME: "demouser"
		Description: (For project stack) Name of the SMStudio user to provision - note this is for
		project stack deployments only, and does not affect the default user deployed by the env stack

	Common usage:

	...::: Bootstraps environment with necessary tools like SAM CLI, cfn-lint, etc. :::...
	$ make init

	...::: Build all SAM based services :::...
	$ make build

	...::: Deploy or re-deploy all SAM based services :::...
	$ make deploy

	...::: Delete all SAM based services :::...
	$ make delete
endef

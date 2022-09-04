
clean: clean-infra clean-cli
	rm -rf scripts/packaging/node_modules

clean-infra:
	cd infra && npm run clean && rm -rf cdk.out

clean-cli:
	cd cli && npm run clean

build-infra:
	cd infra && npm ci && npm run build

build-cli:
	cd cli && npm ci && npm run build

synth-assets: build-infra
	rm -rf ${CURDIR}/example/log_sources/cloudtrail
	cd infra && ./node_modules/aws-cdk/bin/cdk synth \
		--app "node dist/bin/app.js" \
		--context matanoUserDirectory=${CURDIR}/example \
		--context matanoAwsAccountId=123456789012 \
		--context matanoAwsRegion=us-east-1 \
		--context matanoContext="{\"vpc\":{\"vpcId\":\"vpc-05175918865d89771\",\"vpcCidrBlock\":\"172.31.0.0/16\",\"availabilityZones\":[\"us-west-2a\"],\"publicSubnetIds\":[\"subnet-0ed3947530beb444e\"],\"publicSubnetNames\":[\"Public\"],\"publicSubnetRouteTableIds\":[\"rtb-0cb5ed9727a71bb7e\"]}}"

package: build-cli synth-assets
	cd scripts/packaging && npm install pkg@5.8.0 && cd ${CURDIR} && node scripts/packaging/build.js

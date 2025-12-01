.PHONY: apply destroy proxy-apply

apply: secret-oci
	@set -a; . ../.env; set +a; \
	envsubst < nf-core.yml | kubectl apply -f -; \
	envsubst < nf-proxy.yml | kubectl apply -f -

destroy:
	@set -a; . ../.env; set +a; \
	envsubst < nf-proxy.yml | kubectl delete --ignore-not-found -f - || true; \
	envsubst < nf-core.yml | kubectl delete --ignore-not-found -f - || true; \
	kubectl delete namespace nf-server --ignore-not-found || true

reset: destroy apply

proxy-apply:
	@replicas=$${REPLICAS:-$${PROXY_REPLICAS:-2}}; \
	echo "Scaling nf-proxy to $$replicas replicas"; \
	kubectl -n nf-server scale deployment/nf-proxy --replicas=$$replicas


.PHONY: secret-oci
secret-oci:
	@set -e; \
	if [ ! -d ../.oci ]; then echo "../.oci not found"; exit 1; fi; \
	if [ -z "$$(ls -A ../.oci)" ]; then echo "../.oci is empty"; exit 1; fi; \
	kubectl get ns nf-server >/dev/null 2>&1 || kubectl create ns nf-server; \
	kubectl -n nf-server delete secret nf-server-oci 2>/dev/null || true; \
	kubectl -n nf-server create secret generic nf-server-oci \
	  --from-file=bastion_private_key=../.oci/bastion_private_key \
	  --from-file=key.pem=../.oci/key.pem \
	  --from-file=config=../.oci/config; \
	echo "Created nf-server-oci secret with bastion_private_key, key.pem, config"

status:
	watch -n 5 kubectl -n nf-server get pods -o wide

logs:
	kubectl -n nf-server logs -l app=nf-gateway --tail=100 -f
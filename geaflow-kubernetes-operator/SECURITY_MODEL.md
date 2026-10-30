# Kubernetes operator security model

This model describes the operator with mandatory web authentication. It does not apply
retroactively to older operator images. Installing this Helm chart around an old image
cannot add application authentication.

## Trust and permissions

The operator is a privileged workload deployment service. Only trusted cluster operators
may hold its administrator token or create/update GeaFlow custom resources directly.
Submitting a job means authorizing execution of its image, entry class and executable
artifacts, with the job's service account. Job configuration can influence deployment
settings as well. An administrator token is not a safe capability to give to untrusted
code authors or isolated tenants.

The administrator token permits all web API methods. The optional, separate viewer token
permits only GET and HEAD under `/api`. Viewers can see all returned job specifications
and cluster information in the operator's namespace; those responses may contain sensitive
configuration. GET/HEAD endpoints must remain free of mutations. There is no per-job,
per-tenant or individual-user authorization. Tokens are shared deployment credentials,
not Kubernetes credentials, and requests are not impersonated into Kubernetes.

Unauthenticated network clients, including other pods, must not read or mutate business
API resources. Only the GET health response and dashboard static files are public.
Missing, malformed, too-short or identical administrator/viewer credentials prevent startup.
Invalid/missing request credentials return HTTP 401; an authenticated viewer attempting a
write receives HTTP 403. No anonymous fallback or authentication-disable option is provided.

## Configure credentials

Generate independent random tokens with at least 32 random bytes (for example,
`openssl rand -hex 32`). The accepted representation is 32–512 characters using RFC 6750
bearer-token characters. Length validation cannot establish entropy; do not use repeated
characters, example strings or passwords.

For standalone execution set `GEAFLOW_OPERATOR_ADMIN_TOKEN` and optionally
`GEAFLOW_OPERATOR_VIEWER_TOKEN` in the process environment. The equivalent Spring properties
are `geaflow.operator.web.security.admin-token` and `geaflow.operator.web.security.viewer-token`.
Use a secret manager, protected environment or Spring configuration tree. Do not put
credentials in Git, Helm values, command-line arguments, URLs or application logs.

For Kubernetes, create a Secret in the operator's namespace. This example keeps token values
out of command arguments and terminal output. Protect and distribute the files through your
approved secret-management process; the administrator and viewer tokens have different trust
requirements. Disable shell tracing before handling real tokens.

```sh
set +x
umask 077
OPERATOR_SECRET_DIR=$(mktemp -d)
openssl rand -hex 32 | tr -d '\n' > "$OPERATOR_SECRET_DIR/admin-token"
openssl rand -hex 32 | tr -d '\n' > "$OPERATOR_SECRET_DIR/viewer-token"
kubectl create namespace geaflow-system
kubectl -n geaflow-system create secret generic geaflow-operator-web-auth \
  --from-file=admin-token="$OPERATOR_SECRET_DIR/admin-token" \
  --from-file=viewer-token="$OPERATOR_SECRET_DIR/viewer-token"
```

The viewer key can be omitted to disable viewer access. The chart requires
`webSecurity.existingSecret` and `image.tag`; the administrator key is not optional.
Use an immutable, unique image tag built from the patched application, rather than reusing
a tag cached on cluster nodes. If you change Secret key names, set `adminTokenKey` and
`viewerTokenKey` under `webSecurity`.

```sh
helm upgrade --install geaflow-kubernetes-operator \
  geaflow-kubernetes-operator/helm/geaflow-kubernetes-operator \
  --namespace geaflow-system \
  --set webSecurity.existingSecret=geaflow-operator-web-auth \
  --set image.repository=YOUR_PRIVATE_REGISTRY/geaflow-kubernetes-operator \
  --set image.tag=YOUR_UNIQUE_PATCHED_IMAGE_TAG \
  --set-json 'watchNamespaces=["geaflow-system"]'
```

The namespace setting above selects namespaced RBAC in the existing chart. It must match
this operator's current namespace: the reconciler watches the current namespace. Do not
interpret a list in `watchNamespaces` as configuring the reconciler to watch multiple
namespaces. Review effective RoleBindings and ClusterRoleBindings after upgrades, including
resources retained by prior releases. The chart's legacy cluster-scoped RBAC, wildcard
verbs and Secret permissions need a separate least-privilege review against enabled features.

After transferring the tokens securely, delete the temporary files:

```sh
rm -f "$OPERATOR_SECRET_DIR/admin-token" "$OPERATOR_SECRET_DIR/viewer-token"
rmdir "$OPERATOR_SECRET_DIR"
unset OPERATOR_SECRET_DIR
```

## Network and browser access

The chart retains a ClusterIP service and disabled ingress. Its default NetworkPolicy denies
inbound pod traffic. A CNI that enforces NetworkPolicy is required; policies are additive,
so another policy may allow ingress. NetworkPolicy does not replace authentication.
The policy intentionally leaves operator egress unchanged.

Use the Kubernetes-authenticated local tunnel for dashboard access:

```sh
kubectl -n geaflow-system port-forward --address 127.0.0.1 \
  service/geaflow-kubernetes-operator 8089:80
```

Open `http://127.0.0.1:8089/` and enter the viewer token for normal dashboard use. The dashboard
stores its token in page memory only, clears it on disconnect/reload or a 401 response, and
attaches it only to its own API URLs. Do not install untrusted scripts or browser extensions
on an administrator's browser. The static shell being accessible does not grant API access.

For remote access, terminate HTTPS at a trusted ingress/proxy and protect the upstream
connection according to your cluster's trust boundary. Do not send bearer tokens over an
untrusted plaintext connection. If a proxy rewrites a URL prefix, serve the dashboard at a
prefix ending in `/` (or `/index.html`) and forward Authorization unchanged. Forwarded
identity headers alone never authenticate a request. Rate limiting and access logging,
with Authorization redacted, belong at the trusted gateway.

To allow only a designated ingress controller, adapt both selectors and the port to your
cluster, and merge the following with your other Helm values. Supplying both selectors in
one `from` entry requires both to match:

```yaml
networkPolicy:
  enabled: true
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: ingress-nginx
          podSelector:
            matchLabels:
              app.kubernetes.io/component: controller
      ports:
        - protocol: TCP
          port: 8089
```

API clients must send `Authorization: Bearer <token>` explicitly. Cookies, query-string
tokens, Basic authentication and sessions are not accepted. CORS is not enabled. CSRF is
disabled specifically because no browser-implicit authentication mechanism is enabled;
adding cookie/session/Basic authentication requires revisiting CSRF protection.

## Rotation and deployment

Update the externally managed Secret and restart every operator replica. Environment-backed
credentials do not reload when a Secret changes. Rollout completion, not Secret modification,
is the point when old credentials stop working. The chart applies its existing default
`Recreate` strategy, which entails downtime. If you select a rolling strategy, old and new
credentials may remain valid on different replicas during the transition. During the initial
upgrade from an unauthenticated version, isolate the service before deployment and keep it
isolated until every old pod has stopped. Confirm rollout success and readiness before
restoring approved access. Never restore an exposed unauthenticated image as a rollback.

## Workload isolation

Apply least-privilege operator and job service accounts, namespace isolation, Pod Security
Admission and appropriate resource quotas. Restrict executable images, service account
selection, host mounts, host networking and other deployment settings with admission policies
that cover generated pods/deployments and direct custom-resource submission. Do not let
untrusted jobs select the operator service account. An image-only allowlist does not make
user JARs, entry classes or arbitrary job settings safe.

Review the resolved dependencies and the vendor support status before release. This targeted
change retains the existing Spring Boot 2.7 dependency baseline; it is not a framework upgrade.

The web authentication change does not sandbox jobs, remove existing suspicious workloads,
revoke Kubernetes credentials, add admission policies, or establish per-user audit identity.
Investigate unexpected CRs, pods, deployments, service accounts and audit events separately.

Report security issues privately using the [project security policy](../SECURITY.md).

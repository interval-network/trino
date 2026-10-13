/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg.catalog.rest;

import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Manager;
import org.apache.iceberg.rest.auth.OAuth2Util;

import java.time.Duration;

import static com.google.common.base.Preconditions.checkArgument;

/// OAuth2 auth manager for REST catalogs configured with session type NONE.
///
/// With session type NONE, Trino does not propagate the end-user identity to the REST server: the
/// catalog authenticates as a single service principal via the configured credential, and a fresh
/// random `sessionId` is generated for every catalog operation. The upstream [OAuth2Manager] keys
/// its auth session cache on that `sessionId`, so every operation is a cache miss that fetches a new
/// token. Because all operations share one identity, they can instead share the catalog session,
/// avoiding the per-operation token fetch (and the associated cached sessions and scheduled refresh
/// tasks).
///
/// Sharing only kicks in while the catalog session's token expiry is known and comfortably ahead of
/// now. When the expiry is unknown or token refresh is disabled and the token is near expiry, this
/// falls back to the upstream per-operation behavior.
///
/// Interval fork: between those two cases sits a stable fallback session. Iceberg refreshes a token
/// when `min(lifetime / 10, 5 min)` of it remains, so a token shorter than ten minutes (Keycloak's
/// default is five) is refreshed inside the one-minute margin above. Without the fallback, every
/// operation in that window — 30 seconds of every five minutes for a five-minute token — took the
/// per-operation path and fetched its own token. The fallback is one child session cached under a
/// fixed id, so the window costs one token fetch instead of one per operation. It authenticates with
/// the same catalog credential, so the principal is unchanged. It is used only while its own token
/// is fresh by the same test; otherwise the upstream per-operation path still applies, so a child
/// whose refresh failed cannot pin the catalog to an expired token.
public class SharedSessionOAuth2Manager
        extends OAuth2Manager
{
    private static final long REFRESH_SAFETY_MARGIN_MILLIS = Duration.ofMinutes(1).toMillis();
    // One OAuth2Manager (and so one session cache) exists per catalog, so a constant id is per catalog.
    private static final String SHARED_FALLBACK_SESSION_ID = "trino-shared-session";

    public SharedSessionOAuth2Manager(String managerName)
    {
        super(managerName);
    }

    @Override
    public OAuth2Util.AuthSession contextualSession(SessionCatalog.SessionContext context, AuthSession parent)
    {
        checkArgument(context.identity() == null, "expected shared session");

        OAuth2Util.AuthSession oauthParent = (OAuth2Util.AuthSession) parent;
        if (isFreshEnoughToShare(oauthParent)) {
            return oauthParent;
        }

        OAuth2Util.AuthSession sharedFallback = super.contextualSession(
                new SessionCatalog.SessionContext(SHARED_FALLBACK_SESSION_ID, context.identity(), context.credentials(), context.properties(), context.wrappedIdentity()),
                parent);
        if (sharedFallback != oauthParent && isFreshEnoughToShare(sharedFallback)) {
            return sharedFallback;
        }

        return super.contextualSession(context, parent);
    }

    private static boolean isFreshEnoughToShare(OAuth2Util.AuthSession session)
    {
        // A null expiry means the token lifetime is unknown, so we cannot tell whether the shared
        // session is still valid; fall back to the upstream per-operation session in that case.
        Long expiresAt = session.expiresAtMillis();
        return expiresAt != null && expiresAt > System.currentTimeMillis() + REFRESH_SAFETY_MARGIN_MILLIS;
    }
}

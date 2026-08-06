// auth-page.jsx — dedicated /signup and /login pages.
//
// Replaces the login-first modal for the purchase funnel. Two reasons this is a
// page and not a dialog:
//   1. It needs a real URL, so a visitor can be sent straight to /signup from
//      an ad, and so the browser back button behaves.
//   2. It has to carry purchase intent through. Previously the pricing CTA
//      opened the modal and, on success, dropped the user into the app — the
//      thing they had just tried to buy was forgotten.
//
// Everything here assumes the visitor is mid-purchase unless told otherwise:
// signup is the default mode, and the primary button says what happens next
// ("Continue to payment"), never a generic "Submit".

const { useState: useStateAuth, useEffect: useEffectAuth, useRef: useRefAuth } = React;

function GoogleMark() {
  // Google's brand guidelines require the official four-colour mark; a generic
  // "G" or a monochrome icon is a trademark problem.
  return (
    <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
      <path fill="#4285F4" d="M17.64 9.2c0-.64-.06-1.25-.16-1.84H9v3.48h4.84a4.14 4.14 0 0 1-1.8 2.72v2.26h2.92c1.7-1.57 2.68-3.88 2.68-6.62z"/>
      <path fill="#34A853" d="M9 18c2.43 0 4.47-.8 5.96-2.18l-2.92-2.26c-.8.54-1.84.86-3.04.86-2.34 0-4.32-1.58-5.03-3.7H.96v2.33A9 9 0 0 0 9 18z"/>
      <path fill="#FBBC05" d="M3.97 10.72a5.4 5.4 0 0 1 0-3.44V4.95H.96a9 9 0 0 0 0 8.1l3.01-2.33z"/>
      <path fill="#EA4335" d="M9 3.58c1.32 0 2.5.45 3.44 1.35l2.58-2.58C13.46.89 11.43 0 9 0A9 9 0 0 0 .96 4.95l3.01 2.33C4.68 5.16 6.66 3.58 9 3.58z"/>
    </svg>
  );
}

/**
 * @param mode      "signup" | "login"
 * @param intent    optional {type:"checkout", plan} — drives the copy and what
 *                  happens after a successful auth
 * @param onDone    called with the session once authenticated
 * @param onSwitch  swap between signup and login (updates the URL)
 * @param onBack    leave the auth flow
 */
function AuthPage({ mode, intent, onDone, onSwitch, onBack }) {
  const [email, setEmail] = useStateAuth("");
  const [pw, setPw] = useStateAuth("");
  const [err, setErr] = useStateAuth("");
  const [notice, setNotice] = useStateAuth("");
  const [busy, setBusy] = useStateAuth("");   // "" | "email" | "google"
  const firstField = useRefAuth(null);

  const isSignup = mode === "signup";
  const buying = intent && intent.type === "checkout";
  const googleOn = window.cpApi && window.cpApi.googleEnabled();

  useEffectAuth(() => { if (firstField.current) firstField.current.focus(); }, [mode]);

  // Where Google should return to. Intent rides in the URL because the OAuth
  // round-trip leaves our origin and storage written just before a cross-site
  // navigation isn't reliable in Safari.
  //
  // It must be a CLIENT route: the Supabase session comes back in the URL hash
  // and lives in localStorage, so a server-side redirect could not see it. The
  // /checkout route waits for the session to land, then opens Stripe.
  const googleNext = buying ? `/checkout?plan=${encodeURIComponent(intent.plan)}` : "/app";

  const submit = async (e) => {
    e.preventDefault();
    setErr(""); setNotice("");
    if (!email || !pw) { setErr("Enter your email and a password."); return; }
    if (isSignup && pw.length < 8) { setErr("Use at least 8 characters."); return; }

    setBusy("email");
    try {
      if (isSignup) {
        const res = await window.cpApi.signUp(email, pw);
        // No session means Supabase has "Confirm email" switched on. Don't
        // pretend it worked — say exactly what to do, and keep the intent so
        // checkout resumes when they come back.
        if (!res.session) {
          setNotice(
            buying
              ? "Check your email to confirm your account. Come back to this page and log in — we'll take you straight to payment."
              : "Check your email to confirm your account, then log in."
          );
          setBusy("");
          return;
        }
      } else {
        await window.cpApi.signIn(email, pw);
      }
      onDone && onDone();
    } catch (ex) {
      const m = String(ex.message || "");
      // Supabase's raw copy here is unhelpful mid-purchase.
      if (/already registered|already exists/i.test(m)) {
        setErr("That email already has an account. Log in instead.");
      } else if (/invalid login credentials/i.test(m)) {
        setErr("Wrong email or password.");
      } else {
        setErr(m || "Something went wrong. Try again.");
      }
      setBusy("");
    }
  };

  const google = async () => {
    setErr(""); setBusy("google");
    try {
      await window.cpApi.signInWithGoogle(googleNext);   // navigates away
    } catch (ex) {
      setErr(ex.message || "Couldn't start Google sign-in.");
      setBusy("");
    }
  };

  return (
    <main className="au">
      <div className="au-card">
        <button className="au-back" onClick={onBack} type="button">← Back</button>

        <div className="au-logo"><Logo size={48} animated={false} /></div>

        <h1 className="au-h1">
          {buying
            ? (isSignup ? "Create your account" : "Log in to continue")
            : (isSignup ? "Create your account" : "Welcome back")}
        </h1>
        <p className="au-sub">
          {buying
            ? <>One step, then payment. <b>{intent.plan === "yearly" ? "Yearly" : "Monthly"} plan · 7 days free.</b></>
            : (isSignup ? "Free to create. Card only needed to start a trial." : "Sign in to your CoreProp account.")}
        </p>

        {/* Google first: it is the fastest path and the one most people take. */}
        {googleOn && (
          <>
            <button type="button" className="au-google" onClick={google} disabled={!!busy}>
              <GoogleMark />
              <span>{busy === "google" ? "Redirecting…" : `Continue with Google`}</span>
            </button>
            <div className="au-or"><span>or</span></div>
          </>
        )}

        <form className="au-form" onSubmit={submit}>
          <label className="au-label" htmlFor="au-email">Email</label>
          <input
            id="au-email" ref={firstField} className="au-input" type="email" required
            autoComplete="email" placeholder="you@example.com" value={email}
            onChange={e => setEmail(e.target.value)} disabled={!!busy}
          />

          <label className="au-label" htmlFor="au-pw">Password</label>
          <input
            id="au-pw" className="au-input" type="password" required
            autoComplete={isSignup ? "new-password" : "current-password"}
            placeholder={isSignup ? "At least 8 characters" : "Your password"}
            value={pw} onChange={e => setPw(e.target.value)} disabled={!!busy}
          />

          {err && <div className="au-err" role="alert">{err}</div>}
          {notice && <div className="au-notice" role="status">{notice}</div>}

          <button className="au-submit" type="submit" disabled={!!busy}>
            {busy === "email"
              ? "…"
              : buying ? "Continue to payment" : (isSignup ? "Create account" : "Log in")}
          </button>
        </form>

        <p className="au-switch">
          {isSignup
            ? <>Already have an account? <button type="button" className="cp-link" onClick={() => onSwitch("login")}>Log in</button></>
            : <>New here? <button type="button" className="cp-link" onClick={() => onSwitch("signup")}>Create an account</button></>}
        </p>

        {buying && (
          <p className="au-trust">
            Secure checkout by Stripe · Cancel anytime · We never see your card
          </p>
        )}
      </div>
    </main>
  );
}

/**
 * /checkout — a waypoint, not a destination.
 *
 * Opens Stripe as soon as a session exists. It has to tolerate arriving BEFORE
 * the session does: Google's OAuth redirect lands here with the tokens still in
 * the URL hash, and Supabase parses them a tick after mount. Redirecting to
 * signup on the first render would kick the user back out of a flow they just
 * completed.
 */
function CheckoutRedirect({ navigate, loggedIn }) {
  const [err, setErr] = useStateAuth("");
  const started = useRefAuth(false);
  const [gaveUp, setGaveUp] = useStateAuth(false);

  const plan = (() => {
    const q = new URLSearchParams(window.location.search).get("plan");
    if (q === "monthly" || q === "yearly") return q;
    const intent = window.cpApi && window.cpApi.peekIntent();
    return (intent && intent.plan) || "monthly";
  })();

  useEffectAuth(() => {
    if (started.current || !loggedIn || !window.cpApi) return;
    started.current = true;
    window.cpApi.setIntent({ type: "checkout", plan });
    (async () => {
      try {
        await window.cpApi.startCheckout(plan);   // navigates to Stripe
      } catch (e) {
        const m = String(e.message || "");
        setErr(/not configured|503/i.test(m)
          ? "Payments aren't switched on yet. Nothing was charged."
          : (m || "Couldn't open checkout."));
      }
    })();
  }, [loggedIn, plan]);

  // Give the session a few seconds to materialise before assuming it never will.
  useEffectAuth(() => {
    if (loggedIn) return;
    const id = setTimeout(() => setGaveUp(true), 4000);
    return () => clearTimeout(id);
  }, [loggedIn]);

  useEffectAuth(() => {
    if (gaveUp && !loggedIn) navigate("/signup");
  }, [gaveUp, loggedIn]);

  return (
    <main className="au">
      <div className="au-card au-center">
        <div className="au-logo"><Logo size={48} animated={!err} /></div>
        {err ? (
          <>
            <h1 className="au-h1">Checkout didn't open</h1>
            <p className="au-sub">{err}</p>
            <button className="au-submit" onClick={() => navigate("/pricing")}>Back to pricing</button>
          </>
        ) : (
          <>
            <h1 className="au-h1">Taking you to secure checkout…</h1>
            <p className="au-sub">Stripe is loading. Don't refresh this page.</p>
          </>
        )}
      </div>
    </main>
  );
}

/**
 * /welcome — the Stripe return.
 *
 * The subscription webhook can lag the redirect by a second or two, so this
 * never asserts "you're subscribed"; it confirms the payment went through and
 * lets the app's billing poll settle the entitlement.
 */
function WelcomePage({ navigate, locked }) {
  return (
    <main className="au">
      <div className="au-card au-center">
        <div className="au-check" aria-hidden="true">✓</div>
        <h1 className="au-h1">You're in.</h1>
        <p className="au-sub">
          Payment confirmed and your trial has started. A receipt is on its way to your email.
        </p>
        <button className="au-submit" onClick={() => navigate("/app")}>Open CoreProp</button>
        {locked && (
          <p className="au-trust">
            Still showing as locked? Your subscription can take a moment to register —
            refresh in a few seconds.
          </p>
        )}
        <p className="au-trust">Manage or cancel anytime from the pricing page.</p>
      </div>
    </main>
  );
}

Object.assign(window, { AuthPage, CheckoutRedirect, WelcomePage });

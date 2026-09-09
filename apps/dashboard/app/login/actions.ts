"use server";

import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import { ApiError, SESSION_COOKIE, api } from "@/lib/api";

export interface LoginState {
  error?: string;
}

/**
 * Exchange credentials for a token and store it in an httpOnly cookie.
 *
 * The token is set on the *dashboard's* origin, not the API's: a cookie the API
 * sets on its own domain is useless to a dashboard deployed on Vercel while the
 * API is on Render. Doing it here also means the token never touches client
 * JavaScript, so an XSS in the dashboard cannot read the session.
 */
export async function signIn(_previous: LoginState, formData: FormData): Promise<LoginState> {
  const email = String(formData.get("email") ?? "").trim();
  const password = String(formData.get("password") ?? "");

  if (!email || !password) {
    return { error: "Enter an email and password" };
  }

  let token: string;
  let maxAge: number;
  try {
    const result = await api.login(email, password);
    token = result.access_token;
    maxAge = result.expires_in;
  } catch (error) {
    if (error instanceof ApiError) {
      // The API returns one generic message for every failure mode on purpose;
      // pass it through rather than inventing a more specific one here.
      return { error: error.status === 429 ? "Too many attempts. Wait a few minutes." : error.message };
    }
    return { error: "Could not reach the API. It may still be waking up." };
  }

  (await cookies()).set(SESSION_COOKIE, token, {
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    maxAge,
    path: "/",
  });

  redirect("/");
}

export async function signOut(): Promise<void> {
  (await cookies()).delete(SESSION_COOKIE);
  redirect("/login");
}

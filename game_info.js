async function getGameInfo() {
  return await fetch("https://stake.com/_api/graphql", {
    headers: {
      accept: "*/*",
      "accept-language": "zh-CN,zh;q=0.9",
      "access-control-allow-origin": "*",
      "cache-control": "no-cache",
      "content-type": "application/json",
      pragma: "no-cache",
      priority: "u=1, i",
      "sec-ch-ua":
        '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
      "sec-ch-ua-arch": '"arm"',
      "sec-ch-ua-bitness": '"64"',
      "sec-ch-ua-full-version": '"141.0.7390.123"',
      "sec-ch-ua-full-version-list":
        '"Google Chrome";v="141.0.7390.123", "Not?A_Brand";v="8.0.0.0", "Chromium";v="141.0.7390.123"',
      "sec-ch-ua-mobile": "?0",
      "sec-ch-ua-model": '""',
      "sec-ch-ua-platform": '"macOS"',
      "sec-ch-ua-platform-version": '"15.7.1"',
      "sec-fetch-dest": "empty",
      "sec-fetch-mode": "cors",
      "sec-fetch-site": "same-origin",
      "x-language": "zh",
      "x-operation-name": "StartThirdPartyDemoSession",
      "x-operation-type": "query",
      cookie:
        'currency_currency=btc; currency_hideZeroBalances=false; currency_currencyView=crypto; fiat_number_format=en; cookie_consent=false; session_info=undefined; sidebarView=hidden; oddsFormat=decimal; quick_bet_popup=false; sportMarketGroupMap={}; cookie_last_vip_tab=progress; locale=zh; _ga=GA1.1.1911957271.1760273529; intercom-id-cx1ywgf2=e54ae489-3bda-467c-af1c-c5da2ca6ac02; intercom-device-id-cx1ywgf2=427883d4-f1e7-4c72-aaf8-e9f71b75d87a; fullscreen_preference=false; leftSidebarView_v2=minimized; intercom-session-cx1ywgf2=; __cf_bm=WB..L.VAdf5DjEO930.pbtDQY.HOF.q26XHRPCcRnp4-1762533488-1.0.1.1-MqC8RelrT11giOBwmr9HaM4JZvRJg3yqSYtnlarqXyFBABJ6fTOi6hfq4aVdKYAkzDEak5ZCcb4AFjRqV2.n8m.1v17d6bH0nRXkarpH83E; _cfuvid=xUyt.Hv8vWeiWYTI8YV869.tKYEC9J.lPeM1QVQVuI0-1762533488185-0.0.1.1-604800000; cf_clearance=CyWckU7FpWBGChhqhgcBnNDprDDmYpBdNVJETMGTTzw-1762533490-1.2.1.1-k9VKAtyKgiCJg26jXPyX1WlzZ57C1QwbK9zGTqKDRB.rVxpZd.81ykIEy1QxwDfI35jzHvDb73x4TvmMjvqxkiZoMc6U.Ts_LsQ9v.8BF3wDB6L8jXhAPyYDTeA7xj5s8r6nDa9m.mdGYrExHfC8_spqdlrBajdr336GP1BvEKCmWJ.7mSVvyVdrTJqVJHMXIeS8Q0tYeM6.C513UWVAdToKbMZ5hKnU0OXAjPM80UE; mp_e29e8d653fb046aa5a7d7b151ecf6f99_mixpanel=%7B%22distinct_id%22%3A%22%22%2C%22%24device_id%22%3A%22adf2946a-eef8-4b13-95ea-bc6efd4eff00%22%2C%22%24initial_referrer%22%3A%22%24direct%22%2C%22%24initial_referring_domain%22%3A%22%24direct%22%2C%22__mps%22%3A%7B%7D%2C%22__mpso%22%3A%7B%7D%2C%22__mpus%22%3A%7B%7D%2C%22__mpa%22%3A%7B%7D%2C%22__mpu%22%3A%7B%7D%2C%22__mpr%22%3A%5B%5D%2C%22__mpap%22%3A%5B%5D%7D; g_state={"i_l":0,"i_ll":1762533494283,"i_b":"IdApXEfrvOGi0iF1BB8DOv4gkumY69QAbh66jaogVe8"}; _dd_s=aid=8d480e09-9918-40a2-87c9-ecbdb12ab58f&logs=1&id=00bdd2e3-9549-4a6e-bf3c-14b82796adb5&created=1762533490963&expire=1762534690965; _ga_TWGX3QNXGG=GS2.1.s1762533492$o26$g0$t1762533797$j60$l0$h853604586',
      Referer: "https://stake.com/zh/casino/games/massive-rooster-reloaded",
    },
    body: '{"query":"mutation StartThirdPartyDemoSession($slug: String!) {\\n  startThirdPartyDemoSession(slug: $slug) {\\n    config\\n  }\\n}","variables":{"slug":"massive-rooster-reloaded"}}',
    method: "POST",
  })
    .then((res) => res.json())
    .then((ress) => ress.data.startThirdPartyDemoSession.config)
    .then((url) => {
      const gameUrl = new URL(url);
      return {
        sessionID: gameUrl.searchParams.get("sessionID"),
        gameID: gameUrl.searchParams.get("gameID"),
      };
    });
}
var game_info = [];
for (let i = 0; i < 100; i++) {
  getGameInfo().then((ress) => {
    game_info.push(ress);
  });
}
console.log("game_info====>", game_info);
// async function gameAuth({ sessionID, gameID }) {
//   return await fetch("https://rgs.twist-rgs.com/wallet/authenticate", {
//     headers: {
//       accept: "application/json, text/plain, */*",
//       "accept-language": "zh-CN,zh;q=0.9",
//       "cache-control": "no-cache",
//       "content-type": "application/json",
//       pragma: "no-cache",
//       priority: "u=1, i",
//       "sec-ch-ua":
//         '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
//       "sec-ch-ua-mobile": "?0",
//       "sec-ch-ua-platform": '"macOS"',
//       "sec-fetch-dest": "empty",
//       "sec-fetch-mode": "cors",
//       "sec-fetch-site": "cross-site",
//       Referer: "https://live.massivestudios.io/",
//     },
//     body: `{"sessionID":"${sessionID}","gameID":"${gameID}"}`,
//     method: "POST",
//   }).then((res) => res.json());
// }

// gameAuth({
//   sessionID: "0a73d394-31a3-4eee-a250-1ff7b3d41b05",
//   gameID: "11402046-2005-4a98-a78d-b78593272f05",
// });

[
  {
    sessionID: "8fda56ad-9101-4b68-80ee-39d2fc4f8f6e",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "8ebc4c25-abfa-4911-a6fa-96ec6031d0ed",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "043b0e19-e464-4591-b559-374946a108e8",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "0b220c79-fb1a-4b35-aa14-ba6b3321302a",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "c33bd485-b3a1-4fa5-bd19-204a169239eb",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "caaa0306-8e9c-4397-b77c-c3b467a5811e",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "999c76aa-2df2-4ce1-ba74-33fc2a9b9c3b",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "54813501-ea5c-479b-9c85-c39297491f25",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "4ed4e558-a66c-4ebe-93d1-eed7e297c224",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "d30b950d-2c2c-4635-b5e9-87d91ac0ca87",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "dc0a24fc-f74b-4083-9cb0-9fabf43ae734",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "1ec0aa39-2125-4571-b5f5-82c6b945c818",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "18aef255-1084-49a6-aeb1-2d94374d4cf2",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "0cb1fe8f-4174-44e1-a47a-19a32c213cd8",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "e6736753-dfe1-4a02-ac0c-560a29366147",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "81db64b8-aea4-4f2d-8462-220a962a7eac",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "c317dd8c-73e0-4a2c-8b4e-150538091143",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "c79ef9cf-6e17-45c3-abb5-9d487b481dc3",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "d85f1bb0-77a1-4137-a9ca-3ed19b8f8a73",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "055127cf-bc3e-443d-a332-256e101486cb",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "9dfffe22-dc91-4683-8087-8a8978c1c449",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "db8da766-6083-41cc-aa14-eb0cb68e9259",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "596387f2-1d09-4690-936a-0f4566d2ec2a",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "62f55acf-79d7-4e04-b2d3-86c8076ae179",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "b46b048e-4f9b-44cb-a36a-9dd2c7917474",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "c3d05e4b-623b-4db8-832b-5dbbd27897f1",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "4937617a-7f19-4fac-b333-81c79f6847d9",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "6f080ee1-f589-45a3-9765-ed211c92514b",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "938074d3-57e1-4307-a464-729a3ff6dcfd",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "e028da9c-81d0-4f35-8a8b-17e31241226e",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "3ec5e433-f7b2-41df-a953-754c22252c01",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "0ad1c8a2-def5-41ec-b665-9d906067defe",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "b9d8354d-753b-46a0-9e71-628f93ed45e5",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "35e35336-c091-44b1-bdcc-bf963d68bdd9",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "276c4cf8-68c2-428d-80bf-d755e693006d",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "caa1448a-95d2-4dfa-82d4-9cf1229fbbe5",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "64784239-ed35-463a-8774-5b9948a981a8",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "e0fe1806-1138-431f-8527-02dbf95960c1",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "cd38645f-8971-4268-b0cb-7bc75aed9063",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
  {
    sessionID: "0de89a29-044a-47ea-8725-a81b72e484ca",
    gameID: "11402046-2005-4a98-a78d-b78593272f05",
  },
];

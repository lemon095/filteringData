async function getGameInfo() {
  return fetch("https://stake.com/_api/graphql", {
    headers: {
      accept: "*/*",
      "accept-language": "zh-CN,zh;q=0.9",
      "access-control-allow-origin": "*",
      "content-type": "application/json",
      priority: "u=1, i",
      "sec-ch-ua":
        '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
      "sec-ch-ua-arch": '"arm"',
      "sec-ch-ua-bitness": '"64"',
      "sec-ch-ua-full-version": '"143.0.7499.42"',
      "sec-ch-ua-full-version-list":
        '"Google Chrome";v="143.0.7499.42", "Chromium";v="143.0.7499.42", "Not A(Brand";v="24.0.0.0"',
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
        'currency_currency=btc; currency_hideZeroBalances=false; currency_currencyView=crypto; fiat_number_format=en; session_info=undefined; sidebarView=hidden; oddsFormat=decimal; quick_bet_popup=false; sportMarketGroupMap={}; cookie_last_vip_tab=progress; locale=zh; _ga=GA1.1.1911957271.1760273529; intercom-id-cx1ywgf2=e54ae489-3bda-467c-af1c-c5da2ca6ac02; intercom-device-id-cx1ywgf2=427883d4-f1e7-4c72-aaf8-e9f71b75d87a; fullscreen_preference=false; intercom-session-cx1ywgf2=; cookie_consent=true; leftSidebarView_v2=minimized; level_up_vip_flag=; _cfuvid=S9LKatj2UxGrHn6FCKUw1cHEmqGGXdLArgj6ueJkRwc-1766323029626-0.0.1.1-604800000; mp_e29e8d653fb046aa5a7d7b151ecf6f99_mixpanel=%7B%22distinct_id%22%3A%22%22%2C%22%24device_id%22%3A%22adf2946a-eef8-4b13-95ea-bc6efd4eff00%22%2C%22%24initial_referrer%22%3A%22%24direct%22%2C%22%24initial_referring_domain%22%3A%22%24direct%22%2C%22__mps%22%3A%7B%7D%2C%22__mpso%22%3A%7B%7D%2C%22__mpus%22%3A%7B%7D%2C%22__mpa%22%3A%7B%7D%2C%22__mpu%22%3A%7B%7D%2C%22__mpr%22%3A%5B%5D%2C%22__mpap%22%3A%5B%5D%7D; g_state={"i_l":0,"i_ll":1766575457202,"i_b":"3SHOhcnIbygbY1zwkuZA3zPL6v71wENRMVP/2/tm2AM","i_e":{"enable_itp_optimization":0}}; __cf_bm=giMEcnY5mIfeD971nP.YayzlgjclO7GaqBZe5ZKAyYI-1766577206-1.0.1.1-fXd3CGz_UlN8k3ZBfC4Geca1i4MtBPDR4aZIcs4Sk2bdHbCDG6GT8xeblCjhRciMDLPNRGUxs0pWNHwe1v24f_PVVDelVS6a6aYp5rYW.wU; _dd_s=aid=8d480e09-9918-40a2-87c9-ecbdb12ab58f&logs=1&id=599becf3-a5a0-4884-92b7-a81c56b4d835&created=1766575455203&expire=1766578335138; _ga_TWGX3QNXGG=GS2.1.s1766577465$o101$g0$t1766577465$j60$l0$h1784345512; cf_clearance=8hAVs.ayz7XdUB8oioF8nSsS1EMnqRx2dM3AIhZespw-1766577466-1.2.1.1-H46oKdgncXPbqKEDnBhLebCfE5E3b0OsTlDK2pyDjBHDWYadCq2uUyul0HPDADNOrcH7imtUKy.vHfu5lqehovKuRvgxH_Eup8MQXWQV0uZp1HiHyLEN7v0prTT6Z4S2vfsGP0ui6BWfNRY0eyLbnEEZIywzVuruKkH.IV4NGp5kgRZQBipA8MMvIeSnScn7CK4BNHmp_mBdEngvop0nLPXCoLadDvTU9h303.N8aoEuXfEvP3eRBxuSxhRs2SZq',
      Referer: "https://stake.com/zh/casino/games/massive-dracs-stacks",
    },
    body: '{"query":"mutation StartThirdPartyDemoSession($slug: String!) {\\n  startThirdPartyDemoSession(slug: $slug) {\\n    config\\n  }\\n}","variables":{"slug":"massive-dracs-stacks"}}',
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
// console.log("game_info====>", game_info);
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

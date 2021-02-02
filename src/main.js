import Vue from 'vue'
import ElementUI from 'element-ui'
import 'element-ui/lib/theme-chalk/index.css'
import App from './App.vue'
import router from './router'
import store from './store'
import './assets/css/global.css'


Vue.use(ElementUI)
Vue.config.productionTip = false

import axios from 'axios'
Vue.prototype.$axios = axios    //全局注册，使用方法为:this.$axios

new Vue({
  router,
  store,
  render: h => h(App)
}).$mount('#app')
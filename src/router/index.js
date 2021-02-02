import Vue from 'vue'
import VueRouter from 'vue-router'

Vue.use(VueRouter)

const routes = [
  {
    path: '/',
    redirect: '/main'
  },
  {
    path: '/main',
    name: 'Main',
    component: () => import('../views/main.vue'),
    children :[
      {
        path: '/1',
        name: '1',
        component: () => import('../views/1.vue')
      },
      {
        path: '/2',
        name: '2',
        component: () => import('../views/Recom.vue')
      },
      {
        path: '/3',
        name: '3',
        component: () => import('../views/3.vue')
      }
    ]
  }
]

const router = new VueRouter({
  routes
})

export default router
